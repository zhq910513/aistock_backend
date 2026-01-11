from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

SH_TZ = ZoneInfo("Asia/Shanghai")


def now_shanghai() -> datetime:
    return datetime.now(tz=SH_TZ)


def to_shanghai(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(SH_TZ)


def fmt_ts_millis(dt: datetime) -> str:
    dt = to_shanghai(dt)
    return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{int(dt.microsecond / 1000):03d}"


def now_shanghai_str() -> str:
    return fmt_ts_millis(now_shanghai())


def trading_day_str(dt: datetime) -> str:
    dt = to_shanghai(dt)
    return dt.strftime("%Y%m%d")
