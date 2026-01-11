from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from app.config import settings
from app.database.repo import Repo
from app.core.broker_adapter import BrokerAdapter, MockBrokerAdapter


@dataclass
class ExecutionRoute:
    account_id: str
    broker: BrokerAdapter


class ExecutionRouter:
    """
    Multi-account routing (minimal):
    - choose an account_id deterministically for a symbol
    - return a broker adapter bound to that account
    """
    def __init__(self) -> None:
        self._brokers: dict[str, BrokerAdapter] = {}

    def ensure_brokers(self, s: Session) -> None:
        """Ensure broker adapters are instantiated (stable per account_id)."""
        repo = Repo(s)
        repo.accounts.ensure_accounts_seeded()
        for acc in repo.accounts.list_accounts():
            if acc.account_id not in self._brokers:
                self._brokers[acc.account_id] = MockBrokerAdapter(account_id=acc.account_id)

    def list_brokers(self, s: Session) -> list[tuple[str, BrokerAdapter]]:
        self.ensure_brokers(s)
        return sorted(self._brokers.items(), key=lambda x: x[0])

    def route(self, s: Session, symbol: str) -> ExecutionRoute:
        repo = Repo(s)
        repo.accounts.ensure_accounts_seeded()

        accounts = repo.accounts.list_accounts()
        if not accounts:
            account_id = settings.DEFAULT_ACCOUNT_ID
        else:
            idx = abs(hash(symbol)) % len(accounts)
            account_id = accounts[idx].account_id

        # stable broker adapter per account_id (important for mock order/fill state)
        self.ensure_brokers(s)
        broker = self._brokers[account_id]
        return ExecutionRoute(account_id=account_id, broker=broker)
