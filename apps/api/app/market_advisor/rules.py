from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, Iterable, List, Mapping, Optional, Tuple

from .roles import REPARTO_LIMITS, ROLE_REPARTO


@dataclass(frozen=True)
class MarketWindow:
    key: str
    start: date
    end: date
    max_changes: int


MARKET_WINDOWS: Tuple[MarketWindow, ...] = (
    MarketWindow("08-09-2025_11-09-2025", date(2025, 9, 8), date(2025, 9, 11), 5),
    MarketWindow("17-11-2025_20-11-2025", date(2025, 11, 17), date(2025, 11, 20), 4),
    MarketWindow("03-02-2026_06-02-2026", date(2026, 2, 3), date(2026, 2, 6), 5),
    MarketWindow("30-03-2026_02-04-2026", date(2026, 3, 30), date(2026, 4, 2), 3),
)

_INITIAL_BAND_LIMITS = {
    "Dif": {"threshold": 14.0, "max": 2},
    "Cen": {"threshold": 18.0, "max": 2},
    "Att": {"threshold": 24.0, "max": 1},
}


def _normalize_window_key(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    text = text.replace(" ", "")
    text = text.replace("->", "_")
    text = text.replace("/", "-")
    text = text.replace("|", "_")
    text = text.replace("__", "_")
    return text


def _parse_date_token(token: str) -> Optional[date]:
    value = str(token or "").strip()
    if not value:
        return None
    for fmt in ("%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _try_window_from_range_string(window_key: str) -> Optional[MarketWindow]:
    normalized = _normalize_window_key(window_key)
    if "_" not in normalized:
        return None
    left, right = normalized.split("_", 1)
    start = _parse_date_token(left)
    end = _parse_date_token(right)
    if not start or not end:
        return None
    for window in MARKET_WINDOWS:
        if window.start == start and window.end == end:
            return window
    return None


def list_market_windows() -> List[Dict[str, object]]:
    return [
        {
            "key": w.key,
            "start": w.start.isoformat(),
            "end": w.end.isoformat(),
            "max_changes": w.max_changes,
        }
        for w in MARKET_WINDOWS
    ]


def resolve_market_window(window_key: str) -> MarketWindow:
    normalized = _normalize_window_key(window_key)
    by_key = {w.key: w for w in MARKET_WINDOWS}
    if normalized in by_key:
        return by_key[normalized]

    # Also allow selecting by first date only.
    start_only = _parse_date_token(normalized)
    if start_only is not None:
        for window in MARKET_WINDOWS:
            if window.start == start_only:
                return window

    maybe_range = _try_window_from_range_string(normalized)
    if maybe_range is not None:
        return maybe_range

    raise ValueError(f"Finestra mercato non valida: {window_key}")


def reparto_counts(players: Iterable[Mapping[str, object]]) -> Dict[str, int]:
    counts = {"Por": 0, "Dif": 0, "Cen": 0, "Att": 0}
    for player in players:
        role = str(player.get("mantra_role_best") or "").strip()
        reparto = ROLE_REPARTO.get(role)
        if reparto in counts:
            counts[reparto] += 1
    return counts


def club_counts(players: Iterable[Mapping[str, object]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for player in players:
        club = str(player.get("club") or "").strip()
        if not club:
            continue
        out[club] = out.get(club, 0) + 1
    return out


def validate_team_cap(players: Iterable[Mapping[str, object]], cap: int = 3) -> List[str]:
    violations: List[str] = []
    for club, count in sorted(club_counts(players).items(), key=lambda item: (-item[1], item[0])):
        if count > cap:
            violations.append(f"Team-cap violato: {club} ha {count} calciatori (max {cap})")
    return violations


def _player_price_for_band(player: Mapping[str, object]) -> float:
    # Prefer value in roster context, fallback to quotazioni value.
    if "prezzo_attuale_rosa" in player:
        return float(player.get("prezzo_attuale_rosa") or 0.0)
    return float(player.get("prezzo_attuale") or 0.0)


def validate_initial_bands(players: Iterable[Mapping[str, object]]) -> List[str]:
    counters = {
        "Dif": 0,
        "Cen": 0,
        "Att": 0,
    }
    for player in players:
        role = str(player.get("mantra_role_best") or "")
        reparto = ROLE_REPARTO.get(role)
        if reparto not in counters:
            continue
        cfg = _INITIAL_BAND_LIMITS[reparto]
        if _player_price_for_band(player) >= float(cfg["threshold"]):
            counters[reparto] += 1

    violations: List[str] = []
    for reparto, count in counters.items():
        cfg = _INITIAL_BAND_LIMITS[reparto]
        if count > int(cfg["max"]):
            violations.append(
                f"Vincolo fascia iniziale {reparto} violato: {count} >= {cfg['threshold']} (max {cfg['max']})"
            )
    return violations


def validate_roster(
    players: List[Mapping[str, object]],
    *,
    enforce_initial_bands: bool = False,
    team_cap: int = 3,
) -> Tuple[bool, List[str], Dict[str, object]]:
    reasons: List[str] = []
    counts = reparto_counts(players)

    total_players = len(players)
    if total_players != 23:
        reasons.append(f"Rosa non valida: {total_players} giocatori (attesi 23)")

    for reparto, expected in REPARTO_LIMITS.items():
        got = int(counts.get(reparto, 0))
        if got != expected:
            reasons.append(f"Rosa non valida reparto {reparto}: {got} (attesi {expected})")

    reasons.extend(validate_team_cap(players, cap=team_cap))

    if enforce_initial_bands:
        reasons.extend(validate_initial_bands(players))

    details = {
        "total_players": total_players,
        "reparto_counts": counts,
        "team_cap": team_cap,
        "enforce_initial_bands": enforce_initial_bands,
    }
    return len(reasons) == 0, reasons, details


def roster_after_swap(
    roster_players: List[Mapping[str, object]],
    out_keys: Iterable[str],
    in_players: Iterable[Mapping[str, object]],
) -> List[Dict[str, object]]:
    out_set = {str(k) for k in out_keys}
    kept = [dict(player) for player in roster_players if str(player.get("name_key") or "") not in out_set]
    added = [dict(player) for player in in_players]
    return kept + added


def deficits_after_out(
    roster_after_out: List[Mapping[str, object]],
) -> Dict[str, int]:
    counts = reparto_counts(roster_after_out)
    return {
        reparto: max(0, int(expected) - int(counts.get(reparto, 0)))
        for reparto, expected in REPARTO_LIMITS.items()
    }
