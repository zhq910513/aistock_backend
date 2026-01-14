from __future__ import annotations

import re

# Accept:
# - 000001
# - 000001.SZ / 000001.SH
# - 000001.XSHE / 600000.XSHG (common vendor formats)
# - SZ000001 / SH600000 (prefix form)
_SYMBOL_RE = re.compile(r"^[0-9]{6}(?:\.(SZ|SH|XSHE|XSHG))?$", re.IGNORECASE)
_PREFIX_RE = re.compile(r"^(SZ|SH)\s*([0-9]{6})$", re.IGNORECASE)


def normalize_symbol(symbol: str | None) -> str:
    """Normalize A-share symbol.

    - Accepts: "000001", "000001.SZ", "600000", "600000.SH",
               "SZ000001", "SH600000", "000001.XSHE", "600000.XSHG"
    - Returns uppercase, and appends exchange suffix for 6xxxxxx/9xxxxxx -> .SH else .SZ when missing.
    - If the input doesn't match the basic pattern, returns the stripped upper string as-is.
    """
    s = (symbol or "").strip().upper()
    if not s:
        return ""

    # normalize separators/spaces
    s = s.replace(" ", "").replace("-", ".").replace("_", ".")

    # handle prefix forms: SZ000001 / SH600000
    pm = _PREFIX_RE.match(s)
    if pm:
        ex = pm.group(1).upper()
        code = pm.group(2)
        return f"{code}.{ex}"

    m = _SYMBOL_RE.match(s)
    if not m:
        return s

    # If suffix exists, map vendor formats to our canonical .SZ/.SH
    if "." in s:
        code, suffix = s.split(".", 1)
        suffix = suffix.upper()
        if suffix == "XSHE":
            return f"{code}.SZ"
        if suffix == "XSHG":
            return f"{code}.SH"
        return f"{code}.{suffix}"

    # No suffix: infer by leading digit
    if s.startswith(("6", "9")):
        return f"{s}.SH"
    return f"{s}.SZ"
