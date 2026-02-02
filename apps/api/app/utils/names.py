import re
import unicodedata


def strip_star(value: str) -> str:
    base = str(value or "").strip()
    return re.sub(r"\s*\*\s*$", "", base).strip()


def is_starred(value: str) -> bool:
    return str(value or "").strip().endswith("*")


def normalize_name(value: str) -> str:
    value = strip_star(value).lower()
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"[^a-z0-9]+", "", value)
    return value
