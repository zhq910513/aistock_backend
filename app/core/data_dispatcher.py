from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from app.adapters.data_provider import get_data_provider
from app.database.repo import Repo


@dataclass
class DispatchResult:
    sent: int
    received: int
    failed: int


class DataRequestDispatcher:
    """
    Execute pending DataRequests via configured provider and persist DataResponses.
    """
    def __init__(self) -> None:
        self._provider = get_data_provider()

    def pump_once(self, s: Session, limit: int = 50) -> DispatchResult:
        repo = Repo(s)
        pending = repo.data_requests.fetch_pending(limit=limit)
        sent = 0
        received = 0
        failed = 0

        for req in pending:
            try:
                repo.data_requests.mark_sent(req)
                sent += 1

                payload = dict(req.request_payload or {})
                payload["_params_canonical"] = req.params_canonical

                resp = self._provider.call(req.endpoint, payload)

                resp_id = repo.data_responses.write(
                    request_id=req.request_id,
                    provider=req.provider,
                    endpoint=req.endpoint,
                    http_status=resp.http_status,
                    errorcode=resp.errorcode,
                    errmsg=resp.errmsg,
                    quota_context=resp.quota_context,
                    raw=resp.raw,
                    payload_sha256=resp.payload_sha256,
                    data_ts=None,
                )
                req.status = "RECEIVED"
                req.response_id = resp_id
                received += 1
            except Exception as e:
                repo.data_requests.mark_failed(req, str(e))
                failed += 1

        return DispatchResult(sent=sent, received=received, failed=failed)
