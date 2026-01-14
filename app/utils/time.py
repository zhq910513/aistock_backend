from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

SH_TZ = ZoneInfo("Asia/Shanghai")
UTC_TZ = ZoneInfo("UTC")


def now_shanghai() -> datetime:
    return datetime.now(tz=SH_TZ)


def to_shanghai(dt: datetime) -> datetime:
    """
    Convert datetime to Asia/Shanghai.

    Contract:
    - If dt is naive, treat it as Asia/Shanghai local time (NOT UTC).
      This matches the project rule: "all times unified to Asia/Shanghai".
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=SH_TZ)
    return dt.astimezone(SH_TZ)


def to_utc(dt: datetime) -> datetime:
    """
    Convert datetime to UTC.

    Contract:
    - If dt is naive, treat it as Asia/Shanghai local time.
    """
    return to_shanghai(dt).astimezone(UTC_TZ)


def fmt_ts_millis(dt: datetime) -> str:
    dt = to_shanghai(dt)
    return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{int(dt.microsecond / 1000):03d}"


def now_shanghai_str() -> str:
    return fmt_ts_millis(now_shanghai())


def trading_day_str(dt: datetime) -> str:
    dt = to_shanghai(dt)
    return dt.strftime("%Y%m%d")
