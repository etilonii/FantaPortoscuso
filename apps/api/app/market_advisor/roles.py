from __future__ import annotations

import re
from typing import Dict, Iterable, List, Optional, Set


FINAL_ROLES: Set[str] = {
    "Por",
    "Dc",
    "Dd",
    "Ds",
    "B",
    "E",
    "M",
    "C",
    "T",
    "W",
    "A",
    "Pc",
}

ROLE_REPARTO: Dict[str, str] = {
    "Por": "Por",
    "Dc": "Dif",
    "Dd": "Dif",
    "Ds": "Dif",
    "B": "Dif",
    "E": "Cen",
    "M": "Cen",
    "C": "Cen",
    "T": "Cen",
    "W": "Cen",
    "A": "Att",
    "Pc": "Att",
}

REPARTO_LIMITS: Dict[str, int] = {
    "Por": 3,
    "Dif": 7,
    "Cen": 8,
    "Att": 5,
}

TOKEN_TO_ROLE: Dict[str, str] = {
    "POR": "Por",
    "P": "Por",
    "DC": "Dc",
    "D": "Dc",
    "DD": "Dd",
    "DS": "Ds",
    "B": "B",
    "E": "E",
    "M": "M",
    "C": "C",
    "T": "T",
    "W": "W",
    "A": "A",
    "PC": "Pc",
}

ROLE_PRIORITY: List[str] = [
    "Por",
    "Dc",
    "Dd",
    "Ds",
    "B",
    "E",
    "M",
    "C",
    "T",
    "W",
    "A",
    "Pc",
]


def canonical_role(token: str) -> Optional[str]:
    raw = str(token or "").strip()
    if not raw:
        return None
    if raw in FINAL_ROLES:
        return raw
    upper = raw.upper()
    mapped = TOKEN_TO_ROLE.get(upper)
    if mapped in FINAL_ROLES:
        return mapped
    return None


def parse_positions_to_roles(pos_str: str) -> Set[str]:
    raw = str(pos_str or "").strip()
    if not raw:
        return set()
    tokens = re.split(r"[^A-Za-z0-9]+", raw)
    out: Set[str] = set()
    for token in tokens:
        role = canonical_role(token)
        if role:
            out.add(role)
    return out


def reparto_of(role: str) -> Optional[str]:
    return ROLE_REPARTO.get(str(role or "").strip())


def best_role_from_set(roles: Iterable[str]) -> Optional[str]:
    role_set = {role for role in roles if role in FINAL_ROLES}
    if not role_set:
        return None
    if "Por" in role_set:
        return "Por"
    for role in ROLE_PRIORITY:
        if role in role_set:
            return role
    return None


def normalize_role_candidates(*values: str) -> Set[str]:
    out: Set[str] = set()
    for value in values:
        out.update(parse_positions_to_roles(value))
    return out

