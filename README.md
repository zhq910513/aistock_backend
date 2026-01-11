# AIStock_backend（QEE-S³ / S³.1）文件说明
## 根目录

- `main.py`  
  FastAPI 入口；启动时初始化数据库引擎、做连通性检查，并启动 Orchestrator 后台循环；关闭时停止循环任务。

- `requirements.txt`  
  Python 依赖锁定（FastAPI/SQLAlchemy/Alembic/psycopg/crypto/httpx 等）。

- `Dockerfile`  
  Python 3.12 slim 镜像构建脚本；安装依赖、拷贝代码、用 uvicorn 启动服务。

- `docker-compose.yml`  
  本地/服务器一键启动：Postgres + backend；包含端口、环境变量文件、数据卷。

- `.env`  
  运行时配置（最关键：`DATABASE_URL`）；由 pydantic-settings 读取。

- `alembic.ini`  
  Alembic 配置（脚本位置、日志等）；实际数据库 URL 由 `app.config.settings.DATABASE_URL` 注入。

---

## 数据库迁移：`alembic/`

- `alembic/env.py`  
  Alembic 运行入口：加载 `settings.DATABASE_URL`、绑定 `Base.metadata`，支持 offline/online migrations。

- `alembic/versions/0001_init_qee_s3.py`  
  初始化核心表：system_status/system_events/channel_cursor/raw_market_events/orders/trade_fills/decision_bundles/signals/guardian_keys 等（基础骨架）。

- `alembic/versions/0002_channel_fidelity_and_daily.py`  
  ChannelCursor 增加 fidelity 相关字段；新增 `source_fidelity_daily`（盘后跨源对账的日审计表）。

- `alembic/versions/0003_immutability_nonce_outbox_ambiguous.py`  
  新增 nonce_cursor/symbol_locks/outbox_events；并为 system_events/raw_market_events/trade_fills 上“不可变（禁止 UPDATE/DELETE）”触发器。

- `alembic/versions/0004_anchors_frozen_versions_selfcheck.py`  
  新增 `daily_frozen_versions`（盘前版本冻结）；新增 `order_anchors`（多锚点审计钉）；system_status 增加自检门禁字段。

- `alembic/versions/0005_trade_fill_links.py`  
  新增 `trade_fill_links`：用于“裁决后归属成交”的派生映射层（不改 immutable 的 trade_fills）。

---

## 应用配置：`app/config.py`

- `app/config.py`  
  全局 Settings（pydantic-settings）：数据库 URL、冻结版本、门禁开关、调度间隔、S³.1 功能开关等。

---

## API 层：`app/api/`

- `app/api/router.py`  
  HTTP 接口集合：  
  - `/health` 健康检查  
  - `/system/status` 查询系统门禁/自检/熔断状态  
  - `/system/panic_halt` 手动熔断  
  - `/system/reset/request`、`/system/reset/confirm` 双人签署 reset（DevOps/RiskOfficer）+ 写 SELF_CHECK_REPORT  
  - `/events`、`/orders`、`/order_anchors` 基础观测接口

---

## 数据库层：`app/database/`

- `app/database/models.py`  
  SQLAlchemy ORM 模型定义（表结构）；承载 S³ 的“事实表 + 派生表 + 治理表”全集。

- `app/database/engine.py`  
  SQLAlchemy Engine/SessionLocal 初始化；`init_schema_check()` 做最小生产安全连通性校验（SELECT 1）。

- `app/database/repo.py`  
  Repo 聚合：把常用写库操作封装成仓储（SystemEventsRepo/SystemStatusRepo/NonceRepo/SymbolLockRepo/OutboxRepo/OrderAnchorRepo/FrozenVersionsRepo）。

---

## 适配器层：`app/adapters/`

- `app/adapters/ths_adapter.py`  
  THS 数据接入抽象（Protocol）；提供 MockTHSAdapter：生成模拟行情事件、模拟跨源分钟收盘价用于 S³.1 对账。

---

## 核心交易链路：`app/core/`

- `app/core/orchestrator.py`  
  系统心跳主循环（T0 loop）：  
  - 行情 ingest + sequencer（channel_cursor/P99/epsilon/realtime_flag）  
  - 推理 Reasoning → 生成 DecisionBundle / Signal  
  - 交易门禁（self-check + guard + panic）  
  - 创建订单 → outbox 入队  
  - outbox dispatch（模拟 broker）  
  - 成交对账 reconcile（fills-first + ambiguous/orphan）

- `app/core/reasoning.py`  
  推理引擎最小实现（占位）；输出 decision/confidence/reason_code/params，并携带版本信息接口。

- `app/core/guard.py`  
  风控“宪法”：独立一票否决；当前为最小规则（panic_halt、qty 合法性），并支持 ORPHAN_FILL 触发 ORANGE（guard_level=2）。

- `app/core/canonicalization.py`  
  订单规范化协议：绑定 tick/lot 规则、price 离散为 int64（*10000）、market=0；序列化后 SHA256 生成 metadata_hash。

- `app/core/order_manager.py`  
  订单状态机 + outbox 入队：  
  - create_order（写 orders）  
  - transition（写 order_transitions + 更新订单状态）  
  - submit（CREATED→SUBMITTED，并 enqueue SEND_ORDER）

- `app/core/outbox.py`  
  OutboxDispatcher：消费 outbox_events，发送到 broker（mock），写回 orders.broker_order_id，并落 `order_anchors` 多锚点审计钉。

- `app/core/reconciler.py`  
  对账引擎：fills-first 写入 trade_fills；冲突进入 AMBIGUOUS 并锁 symbol；孤儿成交触发 ORPHAN_FILL_ALARM 与 ORANGE；裁决后用 `trade_fill_links` 归属，不改 immutable 的 trade_fills。

- `app/core/source_fidelity.py`  
  S³.1 盘后跨源对账：比较跨源 close_int64，更新 channel_cursor.fidelity_score/low_streak，写 system_events 与 `source_fidelity_daily`。

- `app/core/differential_audit.py`  
  S³.1 差分审计：当新旧策略合约产生“相反/偏离”决策时写 `CONTRA_DECISION_EVENT`（ShadowLive 审计链）。

---

## 工具箱：`app/utils/`

- `app/utils/time.py`  
  全系统时间工具（Asia/Shanghai）：now、时区转换、毫秒格式化、trading_day（YYYYMMDD）。

- `app/utils/crypto.py`  
  SHA256 工具 + Ed25519 签名验证（reset 双签门禁使用）。

- `app/utils/ids.py`  
  request_id/transition_id 生成；CID 生成（用 trading_day/symbol/strategy_id/signal_ts/nonce/side/qty canonical 化后 sha256）。

- `app/utils/p2.py`  
  P² 分位数估计器：用于 latency P99 在线估计，支撑 epsilon 动态门槛。

- `app/utils/event_bus.py`  
  异步事件总线（非核心链路任务不要阻塞交易链路）。

- `app/utils/scheduler.py`  
  线程池调度器（按标的并行扫描的基础设施，当前为可选骨架）。

---

## 其它脚本（你贴过的“模型生成器占位”）

- `versions/*`（或你实际放置脚本的目录）  
  这类脚本通常用于：生成/打印 `MODEL_SNAPSHOT_UUID`、构造占位模型快照、方便你先跑通“版本冻结/配置链路”。  
  **如果项目还没启动、也不打算跑这些脚本**：可以不进镜像、不影响服务启动；但保留它对“零开始上线前的自检与版本冻结演练”很有用。

---

## 包初始化文件（若存在且为空）

- `app/__init__.py`、`app/api/__init__.py`、`app/core/__init__.py`、`app/database/__init__.py`、`app/utils/__init__.py` 等  
  标记 Python package；内容为空是正常的（不影响运行）。
