import hashlib
import html
import json
import re
import unicodedata
from datetime import date, datetime, timezone


def sha1(*parts) -> str:
    return hashlib.sha1("\x1f".join(str(p) for p in parts).encode("utf-8")).hexdigest()[:20]


def clean(v) -> str | None:
    if v is None:
        return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    return s or None


def strip_html(v) -> str | None:
    if not v:
        return None
    s = re.sub(r"<(br|/p|/div|/li|/h\d)\s*/?>", "\n", str(v), flags=re.I)
    s = html.unescape(re.sub(r"<[^>]+>", " ", s))
    s = "\n".join(re.sub(r"[ \t\xa0]+", " ", ln).strip() for ln in s.splitlines())
    return re.sub(r"\n{3,}", "\n\n", s).strip() or None


def to_float(v) -> float | None:
    try:
        f = float(str(v).replace(",", "")) if v not in (None, "") else None
    except ValueError:
        return None
    return f if f == f else None


def to_int(v) -> int | None:
    f = to_float(v)
    return int(f) if f is not None else None


def parse_ts(v) -> datetime | None:
    if v in (None, "", 0):
        return None
    try:
        if isinstance(v, (int, float)) or str(v).isdigit():
            n = float(v)
            return datetime.fromtimestamp(n / 1000 if n > 1e11 else n, tz=timezone.utc)
        d = datetime.fromisoformat(str(v).replace("Z", "+00:00").replace(" ", "T", 1))
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except (ValueError, OverflowError, OSError):
        return None


def to_date(v) -> date:
    return date.fromisoformat(str(v))


def js(obj) -> str | None:
    obj = {k: v for k, v in obj.items() if v not in (None, "", [], {})}
    return json.dumps(obj, ensure_ascii=False) if obj else None


def fold(s: str) -> str:
    s = unicodedata.normalize("NFD", s.lower().replace("đ", "d"))
    return "".join(c for c in s if unicodedata.category(c) != "Mn")


_KIDS = re.compile(r"\b(kids?|boys?|girls?|baby|babies|toddlers?|infants?|junior|tre em|be trai|be gai|nhi dong|thieu nhi|children|child)\b")
_WOMEN = re.compile(r"\b(women'?s?|woman|womens|ladies|lady|female|nu|nu gioi|vay|dam|ao yem|chan vay|ladieswear|girl|wanita|cewek|perempuan)\b")
_MEN = re.compile(r"\b(men'?s?|man|mens|male|nam|nam gioi|menswear|pria|cowok|laki)\b")


def infer_audience(*texts) -> str:
    t = fold(" ".join(str(x) for x in texts if x))
    if re.search(r"\bunisex\b", t):
        return "unisex"
    if _KIDS.search(t):
        return "kids"
    w, m = bool(_WOMEN.search(t)), bool(_MEN.search(t))
    return "unisex" if w and m else "women" if w else "men" if m else "unknown"


_COLOR = re.compile(r"\b(mau|color|colour|colorway)\b")


def option_role(name: str | None) -> str | None:
    t = fold(name or "")
    if _COLOR.search(t):
        return "color"
    if re.search(r"\b(size|kich|co)\b", t):
        return "size"
    return None


def cat_row(source: str, cid, name, parent=None, level=None, path=None, url=None, n_products=None) -> dict:
    return {"category_key": f"{source}:{cid}", "source_category_id": str(cid), "name": clean(name),
            "parent_key": f"{source}:{parent}" if parent not in (None, "", 0) else None, "level": level, "path": path,
            "url": url, "n_products": n_products}
