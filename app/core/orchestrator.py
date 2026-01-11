from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import select, func
from sqlalchemy.orm import Session

from app.config import settings
from app.adapters.ths_adapter import get_ths_adapter
from app.core.agent_loop import AgentLoop
from app.core.guard import Guard
from app.core.order_manager import OrderManager
from app.core.reconciler import Reconciler
from app.core.outbox import OutboxDispatcher
from app.core.execution_router import ExecutionRouter
from app.core.exit_monitor import ExitMonitor
from app.database.engine import SessionLocal
from app.database.repo import Repo
from app.database import models
from app.utils.time import now_shanghai, trading_day_str, fmt_ts_millis, to_shanghai
from app.utils.ids import make_cid
from app.utils.crypto import sha256_hex
from app.utils.p2 import P2Quantile


@dataclass
class Orchestrator:
    running: bool = True

    def __post_init__(self) -> None:
        self.adapter = get_ths_adapter()
        self.agent = AgentLoop()
        self.guard = Guard()
        self.order_manager = OrderManager()
        self.reconciler = Reconciler()
        self.exec_router = ExecutionRouter()
        self.exit_monitor = ExitMonitor()
        self.outbox_dispatcher = OutboxDispatcher(router=self.exec_router)

        self.symbols = [x.strip() for x in settings.ORCH_SYMBOLS.split(",") if x.strip()]
        self._last_tick_ts = None

    def stop(self) -> None:
        self.running = False

    async def run(self) -> None:
        while self.running:
            await self._tick()
            await asyncio.sleep(settings.ORCH_LOOP_INTERVAL_MS / 1000.0)

    def _trade_gate_ok(self, repo: Repo) -> bool:
        st = repo.system_status.get_for_update()

        if st.veto or st.guard_level >= 2 or st.panic_halt:
            return False

        if not settings.REQUIRE_SELF_CHECK_FOR_TRADING:
            return True

        if not st.last_self_check_report_hash or not st.last_self_check_time:
            return False

        age = (now_shanghai() - st.last_self_check_time).total_seconds()
        return age <= float(settings.SELF_CHECK_MAX_AGE_SEC)


    def _execution_controls_ok(self, repo: Repo, symbol: str, account_id: str | None, data_status: str) -> tuple[bool, str]:
        """Runtime controls gate. Returns (ok, reason)."""
        c = repo.controls.get_for_update()

        if not bool(c.auto_trading_enabled):
            return False, "auto_trading_disabled"

        # symbol allow/deny lists
        blocked = set((c.blocked_symbols or []))
        if symbol in blocked:
            return False, "symbol_blocked"

        allowed = list(c.allowed_symbols or [])
        if allowed and symbol not in set(allowed):
            return False, "symbol_not_in_allowlist"

        # data quality gate
        if bool(c.only_when_data_ok) and (data_status or "") not in {"VALID", "OK"}:
            return False, "data_not_ok"

        # daily order limit (best-effort)
        try:
            now_dt = now_shanghai()
            start = now_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            end = start + timedelta(days=1)
            q = select(func.count(models.Order.cid)).where(models.Order.account_id == (account_id or settings.DEFAULT_ACCOUNT_ID))
            q = q.where(models.Order.created_at >= start, models.Order.created_at < end)
            cnt = int(repo.s.execute(q).scalar_one() or 0)
            if cnt >= int(c.max_orders_per_day):
                return False, "max_orders_per_day_reached"
        except Exception:
            # do not hard-fail on counting
            pass

        return True, "ok"


    async def _tick(self) -> None:
        tick_now = now_shanghai()

        # drift monitoring
        with SessionLocal() as s0:
            repo0 = Repo(s0)
            if self._last_tick_ts is not None:
                real_ms = int(round((tick_now - self._last_tick_ts).total_seconds() * 1000))
                if abs(real_ms - int(settings.ORCH_LOOP_INTERVAL_MS)) >= int(settings.SCHED_DRIFT_THRESHOLD_MS):
                    repo0.system_events.write_event(
                        event_type="SCHEDULER_DRIFT",
                        correlation_id=None,
                        severity="WARN",
                        payload={"expected_ms": settings.ORCH_LOOP_INTERVAL_MS, "real_ms": real_ms},
                    )
                    s0.commit()
            self._last_tick_ts = tick_now

        # 1) ingest + agentic decision + enqueue orders
        with SessionLocal() as s:
            repo = Repo(s)
            repo.accounts.ensure_accounts_seeded()

            # 9.1 Frozen daily versions
            try:
                repo.frozen_versions.ensure_today_frozen(
                    rule_set_version_hash=settings.RULESET_VERSION_HASH,
                    strategy_contract_hash=settings.STRATEGY_CONTRACT_HASH,
                    model_snapshot_uuid=settings.MODEL_SNAPSHOT_UUID,
                    cost_model_version=settings.COST_MODEL_VERSION,
                    canonicalization_version=settings.CANONICALIZATION_VERSION,
                    feature_extractor_version=settings.FEATURE_EXTRACTOR_VERSION,
                )
            except Exception as e:
                repo.system_events.write_event(
                    event_type="DAILY_FROZEN_VERSIONS_MISMATCH",
                    correlation_id=None,
                    severity="CRITICAL",
                    payload={"error": str(e)},
                )
                repo.system_status.set_panic_halt("FROZEN_VERSIONS_MISMATCH")
                s.commit()
                return

            for sym in self.symbols:
                if repo.symbol_lock.is_locked(sym, account_id="GLOBAL"):
                    repo.system_events.write_event(
                        event_type="SYMBOL_LOCKED_SKIP",
                        correlation_id=None,
                        severity="WARN",
                        symbol=sym,
                        payload={"reason": "symbol_locked"},
                    )
                    continue

                ev = self.adapter.fetch_market_event(sym)
                raw_row = self._ingest_event_with_sequencer(s, ev)

                # correlation_id for this tick+symbol decision chain
                corr = sha256_hex(f"{sym}|{raw_row.payload_sha256}|{tick_now.isoformat()}".encode("utf-8"))[:32]

                # route account early (multi-account)
                route = self.exec_router.route(s, sym)
                account_id = route.account_id

                agent_dec = self.agent.run_for_symbol(s, sym, account_id=account_id, correlation_id=corr)

                decision_id = sha256_hex(
                    f"{sym}|{raw_row.payload_sha256}|{agent_dec.feature_hash}|{agent_dec.reason_code}|{agent_dec.decision}".encode("utf-8")
                )

                # rewrite validation record with final decision_id (immutability via new record)
                val_id = repo.validations.write(
                    decision_id=decision_id,
                    symbol=sym,
                    hypothesis=str(agent_dec.params.get("hypothesis", "")),
                    request_ids=agent_dec.request_ids,
                    evidence={"features_hash": agent_dec.feature_hash, "params": agent_dec.params},
                    conclusion=str(agent_dec.params.get("validation_conclusion", "INCONCLUSIVE")),
                    score=float(agent_dec.confidence),
                )

                s.add(
                    models.DecisionBundle(
                        decision_id=decision_id,
                        cid=None,
                        account_id=account_id,
                        symbol=sym,
                        decision=agent_dec.decision,
                        reason_code=agent_dec.reason_code,
                        params=agent_dec.params,
                        request_ids=agent_dec.request_ids,
                        model_hash=agent_dec.model_hash,
                        feature_hash=agent_dec.feature_hash,
                        seed_set_hash="",
                        rng_seed_hash="",
                        guard_status={},
                        data_quality={
                            "data_status": raw_row.data_status,
                            "latency_ms": raw_row.latency_ms,
                            "completion_rate": raw_row.completion_rate,
                            "realtime_flag": raw_row.realtime_flag,
                            "audit_flag": raw_row.audit_flag,
                        },
                        rule_set_version_hash=settings.RULESET_VERSION_HASH,
                        model_snapshot_uuid=settings.MODEL_SNAPSHOT_UUID,
                        strategy_contract_hash=settings.STRATEGY_CONTRACT_HASH,
                        feature_extractor_version=settings.FEATURE_EXTRACTOR_VERSION,
                        cost_model_version=settings.COST_MODEL_VERSION,
                        lineage_ref=raw_row.payload_sha256,
                        created_at=now_shanghai(),
                    )
                )

                if raw_row.audit_flag and raw_row.realtime_flag and (not raw_row.research_only):
                    s.add(
                        models.TrainingFeatureRow(
                            symbol=sym,
                            data_ts=raw_row.data_ts,
                            ingest_ts=raw_row.ingest_ts,
                            audit_flag=True,
                            realtime_equivalent=True,
                            payload_sha256=raw_row.payload_sha256,
                            channel_id=raw_row.channel_id,
                            channel_seq=raw_row.channel_seq,
                            source_clock_quality=raw_row.source_clock_quality,
                            feature_extractor_version=settings.FEATURE_EXTRACTOR_VERSION,
                            rule_set_version_hash=settings.RULESET_VERSION_HASH,
                            strategy_contract_hash=settings.STRATEGY_CONTRACT_HASH,
                            features={"price": float(raw_row.payload.get("price", 0.0))},
                            created_at=now_shanghai(),
                        )
                    )

                if agent_dec.decision in {"BUY", "SELL"}:
                    # Always record model intent as a Signal (even if execution is blocked)
                    signal_dt = now_shanghai()
                    td = trading_day_str(signal_dt)
                    nonce = repo.nonce.next_nonce(sym, "PRIMARY", account_id=account_id)
                    cid = make_cid(
                        trading_day=td,
                        symbol=sym,
                        strategy_id="PRIMARY",
                        signal_ts=signal_dt,
                        nonce=nonce,
                        side=agent_dec.decision,
                        intended_qty_or_notional=100,
                    )
                    existing_signal = s.get(models.Signal, cid)
                    if existing_signal is None:
                        s.add(
                            models.Signal(
                                cid=cid,
                                account_id=account_id,
                                trading_day=td,
                                symbol=sym,
                                strategy_id="PRIMARY",
                                signal_ts=signal_dt,
                                nonce=nonce,
                                side=agent_dec.decision,
                                intended_qty_or_notional=100,
                                confidence=agent_dec.confidence,
                                rule_set_version_hash=settings.RULESET_VERSION_HASH,
                                strategy_contract_hash=settings.STRATEGY_CONTRACT_HASH,
                                model_snapshot_uuid=settings.MODEL_SNAPSHOT_UUID,
                                cost_model_version=settings.COST_MODEL_VERSION,
                                feature_extractor_version=settings.FEATURE_EXTRACTOR_VERSION,
                                lineage_ref=raw_row.payload_sha256,
                                created_at=now_shanghai(),
                            )
                        )
                
                    # bind decision->cid (best-effort) even if we don't execute an order
                    s.execute(
                        models.DecisionBundle.__table__.update()
                        .where(models.DecisionBundle.decision_id == decision_id)
                        .values(cid=cid)
                    )
                
                    # Guard first (risk veto)
                    gr = self.guard.evaluate(s, sym, agent_dec.decision, intended_qty=100, account_id=account_id)
                    if gr.veto:
                        repo.system_events.write_event(
                            event_type="GUARD_VETO",
                            correlation_id=cid,
                            severity="WARN",
                            symbol=sym,
                            payload={"guard_level": gr.guard_level, "veto_code": gr.veto_code, "account_id": account_id},
                        )
                        continue
                
                    # Governance gate (PANIC/VETO + SELF_CHECK)
                    if not self._trade_gate_ok(repo):
                        repo.system_events.write_event(
                            event_type="TRADE_GATE_BLOCKED",
                            correlation_id=corr,
                            severity="WARN",
                            symbol=sym,
                            payload={"reason": "guard_or_self_check"},
                        )
                        continue
                
                    # Runtime execution controls (AUTO_TRADING, DRY_RUN, limits, allow/deny lists)
                    ctrl_ok, ctrl_reason = self._execution_controls_ok(repo, sym, account_id=account_id, data_status=raw_row.data_status)
                    if not ctrl_ok:
                        repo.system_events.write_event(
                            event_type="EXECUTION_BLOCKED",
                            correlation_id=cid,
                            severity="INFO",
                            symbol=sym,
                            payload={"reason": ctrl_reason, "account_id": account_id},
                        )
                        continue
                
                    self.order_manager.create_order(
                        s=s,
                        cid=cid,
                        account_id=account_id,
                        symbol=sym,
                        side=agent_dec.decision,
                        order_type="LIMIT",
                        limit_price=float(raw_row.payload.get("price", 0.0)),
                        qty=100,
                        strategy_contract_hash=settings.STRATEGY_CONTRACT_HASH,
                    )
                
                    # DRY_RUN means we create orders but do not submit to the outbox/broker
                    if not repo.controls.get_for_update().dry_run:
                        self.order_manager.submit(s, cid)
                    else:
                        repo.system_events.write_event(
                            event_type="DRY_RUN_ORDER_CREATED",
                            correlation_id=cid,
                            severity="INFO",
                            symbol=sym,
                            payload={"note": "order created but not submitted (dry_run=true)", "account_id": account_id},
                        )


            s.commit()

        # 2) outbox dispatch
        with SessionLocal() as s2:
            sent = self.outbox_dispatcher.pump_once(s2, limit=50)
            if sent:
                s2.commit()
            else:
                s2.rollback()

        # 3) poll broker fills -> reconcile -> update portfolio positions
        with SessionLocal() as s3:
            self.exec_router.ensure_brokers(s3)
            total_fills = 0
            for account_id, broker in self.exec_router.list_brokers(s3):
                fills = broker.query_fills()
                if not fills:
                    continue
                total_fills += len(fills)
                self.reconciler.upsert_fills_first(s3, fills)

            if total_fills:
                Repo(s3).system_events.write_event(
                    event_type="BROKER_FILLS_INGESTED",
                    correlation_id=None,
                    severity="INFO",
                    payload={"fills": int(total_fills)},
                )
                s3.commit()
            else:
                s3.rollback()

        # 4) exit monitor: scan open positions -> enqueue SELL when policy triggers
        with SessionLocal() as s4:
            repo4 = Repo(s4)
            repo4.accounts.ensure_accounts_seeded()

            open_positions = (
                s4.execute(select(models.PortfolioPosition).where(models.PortfolioPosition.current_qty > 0))
                .scalars()
                .all()
            )
            if not open_positions:
                s4.rollback()
                return

            for pos in open_positions:
                account_id = str(pos.account_id)
                symbol = str(pos.symbol)
                qty = int(pos.current_qty)
                if qty <= 0:
                    continue

                # prevent duplicate exit orders
                existing = (
                    s4.execute(
                        select(models.Order)
                        .where(
                            models.Order.account_id == account_id,
                            models.Order.symbol == symbol,
                            models.Order.side == "SELL",
                            models.Order.state.in_(["CREATED", "SUBMITTED", "PENDING", "PARTIALLY_FILLED"]),
                        )
                        .limit(1)
                    )
                    .scalars()
                    .first()
                )
                if existing is not None:
                    continue

                dec = self.exit_monitor.evaluate(s4, account_id=account_id, symbol=symbol)
                if dec.decision != "SELL":
                    continue

                if not self._trade_gate_ok(repo4):
                    repo4.system_events.write_event(
                        event_type="EXIT_TRADE_GATE_BLOCKED",
                        correlation_id=None,
                        severity="WARN",
                        symbol=symbol,
                        payload={"account_id": account_id, "reason_code": dec.reason_code},
                    )
                    continue

                # Sell signal/order (CID is idempotency key)
                signal_dt = now_shanghai()
                td = trading_day_str(signal_dt)
                signal_ts_millis = fmt_ts_millis(signal_dt)
                nonce = repo4.nonce.next_nonce(symbol, "EXIT", account_id=account_id)

                cid = make_cid(
                    trading_day=td,
                    symbol=symbol,
                    strategy_id="EXIT",
                    signal_ts_millis=signal_ts_millis,
                    nonce=nonce,
                    side="SELL",
                    intended_qty_or_notional=qty,
                    account_id=account_id,
                )

                # validation record tied to cid
                repo4.validations.write(
                    decision_id=cid,
                    symbol=symbol,
                    hypothesis=f"ExitPolicy({dec.params.get('policy', {}).get('policy_hash', '')})",
                    request_ids=list(dec.params.get("request_ids", []) or []),
                    evidence=dec.params,
                    conclusion="PASS" if dec.confidence >= 0.8 else "INCONCLUSIVE",
                    score=float(dec.confidence),
                )

                # best-effort lineage: latest market payload hash
                last_ev = (
                    s4.execute(
                        select(models.RawMarketEvent)
                        .where(models.RawMarketEvent.symbol == symbol)
                        .order_by(models.RawMarketEvent.data_ts.desc())
                        .limit(1)
                    )
                    .scalars()
                    .first()
                )
                lineage_ref = str((dec.params.get("features", {}) or {}).get("last_market_payload_sha256", ""))

                s4.add(
                    models.DecisionBundle(
                        decision_id=cid,
                        cid=cid,
                        account_id=account_id,
                        symbol=symbol,
                        decision="SELL",
                        reason_code=dec.reason_code,
                        params=dec.params,
                        request_ids=list(dec.params.get("request_ids", []) or []),
                        model_hash=str(dec.params.get("policy", {}).get("policy_hash", "") or ""),
                        feature_hash=str(dec.params.get("feature_hash", "") or ""),
                        seed_set_hash="",
                        rng_seed_hash="",
                        guard_status={},
                        data_quality={},
                        rule_set_version_hash=settings.RULESET_VERSION_HASH,
                        model_snapshot_uuid=settings.MODEL_SNAPSHOT_UUID,
                        strategy_contract_hash=settings.STRATEGY_CONTRACT_HASH,
                        feature_extractor_version=settings.FEATURE_EXTRACTOR_VERSION,
                        cost_model_version=settings.COST_MODEL_VERSION,
                        lineage_ref=lineage_ref,
                        created_at=now_shanghai(),
                    )
                )

                s4.add(
                    models.Signal(
                        cid=cid,
                        account_id=account_id,
                        trading_day=td,
                        symbol=symbol,
                        strategy_id="EXIT",
                        signal_ts=signal_dt,
                        nonce=nonce,
                        side="SELL",
                        intended_qty_or_notional=qty,
                        confidence=float(dec.confidence),
                        rule_set_version_hash=settings.RULESET_VERSION_HASH,
                        strategy_contract_hash=settings.STRATEGY_CONTRACT_HASH,
                        model_snapshot_uuid=settings.MODEL_SNAPSHOT_UUID,
                        cost_model_version=settings.COST_MODEL_VERSION,
                        feature_extractor_version=settings.FEATURE_EXTRACTOR_VERSION,
                        lineage_ref=lineage_ref,
                        created_at=now_shanghai(),
                    )
                )

                # use latest price as limit
                last_price = float((dec.params.get("features", {}) or {}).get("last_price", 0.0))
                if last_price <= 0:
                    last_price = 0.0

                gr = self.guard.evaluate(s4, symbol, "SELL", intended_qty=qty, account_id=account_id)
                if gr.veto:
                    repo4.system_events.write_event(
                        event_type="EXIT_GUARD_VETO",
                        correlation_id=cid,
                        severity="WARN",
                        symbol=symbol,
                        payload={"account_id": account_id, "guard_level": gr.guard_level, "veto_code": gr.veto_code},
                    )
                    continue

                self.order_manager.create_order(
                    s=s4,
                    cid=cid,
                    account_id=account_id,
                    symbol=symbol,
                    side="SELL",
                    order_type="LIMIT",
                    limit_price=last_price if last_price > 0 else None,
                    qty=qty,
                    strategy_contract_hash=settings.STRATEGY_CONTRACT_HASH,
                )
                self.order_manager.submit(s4, cid)

                repo4.system_events.write_event(
                    event_type="EXIT_ORDER_ENQUEUED",
                    correlation_id=cid,
                    severity="INFO",
                    symbol=symbol,
                    payload={"account_id": account_id, "qty": qty, "reason_code": dec.reason_code},
                )

            s4.commit()

    def _ingest_event_with_sequencer(self, s: Session, ev: dict) -> models.RawMarketEvent:
        repo = Repo(s)

        data_ts = ev["data_ts"]
        ingest_ts = ev["ingest_ts"]
        latency_ms = int(round((ingest_ts - data_ts).total_seconds() * 1000))
        ev["latency_ms"] = max(0, latency_ms)

        channel_id = str(ev["channel_id"])
        seq = int(ev["channel_seq"])

        cur = s.get(models.ChannelCursor, channel_id)
        if cur is None:
            q = P2Quantile(q=0.99)
            q.update(float(ev["latency_ms"]))
            cur = models.ChannelCursor(
                channel_id=channel_id,
                last_seq=seq,
                last_ingest_ts=ingest_ts,
                quality_score=1.0,
                p99_latency_ms=max(settings.EPSILON_MIN_MS, int(round(q.value()))),
                p99_state=q.to_state(),
                fidelity_score=1.0,
                fidelity_low_streak=0,
                updated_at=now_shanghai(),
            )
            s.add(cur)
            s.flush()

        jitter = (seq <= int(cur.last_seq)) or (ingest_ts < cur.last_ingest_ts)

        q = P2Quantile.from_state(cur.p99_state or {"q": 0.99})
        q.update(float(ev["latency_ms"]))
        cur.p99_state = q.to_state()
        cur.p99_latency_ms = max(settings.EPSILON_MIN_MS, int(round(q.value())))
        cur.updated_at = now_shanghai()

        epsilon_ms = max(int(cur.p99_latency_ms), int(settings.EPSILON_MIN_MS))
        realtime_flag = (ingest_ts <= data_ts + timedelta(milliseconds=epsilon_ms))

        row = models.RawMarketEvent(
            api_schema_version=str(ev["api_schema_version"]),
            source=str(ev["source"]),
            ths_product=str(ev["ths_product"]),
            ths_function=str(ev["ths_function"]),
            ths_indicator_set=str(ev["ths_indicator_set"]),
            ths_params_canonical=str(ev["ths_params_canonical"]),
            ths_errorcode=str(ev["ths_errorcode"]),
            ths_quota_context=str(ev.get("ths_quota_context", "")),
            source_clock_quality=str(ev["source_clock_quality"]),
            channel_id=channel_id,
            channel_seq=seq,
            symbol=str(ev["symbol"]),
            data_ts=data_ts,
            ingest_ts=ingest_ts,
            payload=ev["payload"],
            payload_sha256=str(ev["payload_sha256"]),
            data_status=str(ev["data_status"]),
            latency_ms=int(ev["latency_ms"]),
            completion_rate=float(ev["completion_rate"]),
            realtime_flag=bool(realtime_flag),
            audit_flag=True,
            research_only=bool(jitter),
            request_id=str(ev["request_id"]),
            producer_instance=str(ev["producer_instance"]),
        )
        s.add(row)

        if jitter:
            cur.quality_score = max(0.0, float(cur.quality_score) - 0.1)
            repo.system_events.write_event(
                event_type="DATA_DEGRADED",
                correlation_id=row.request_id,
                severity="WARN",
                symbol=row.symbol,
                payload={
                    "reason": "Channel_Jitter",
                    "channel_id": channel_id,
                    "seq": seq,
                    "last_seq": int(cur.last_seq),
                    "epsilon_ms": epsilon_ms,
                },
            )
        else:
            cur.last_seq = seq
            cur.last_ingest_ts = ingest_ts

        return row
