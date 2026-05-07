from __future__ import annotations

from typing import Dict, List, Optional


def _safe_int(value: object) -> Optional[int]:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed


def _safe_float(value: object) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed


def build_team_trend_payload(team_name: str, rows: List[Dict[str, object]]) -> Dict[str, object]:
    normalized_rows: List[Dict[str, object]] = []

    for row in rows:
        if not isinstance(row, dict):
            continue
        round_value = _safe_int(row.get("round"))
        score_value = _safe_float(row.get("score"))
        if round_value is None or round_value <= 0 or score_value is None:
            continue
        position_value = _safe_int(row.get("position"))
        live_status = str(row.get("live_status") or "").strip().lower()
        normalized_rows.append(
            {
                "round": int(round_value),
                "score": round(float(score_value), 2),
                "position": int(position_value) if position_value is not None and position_value > 0 else None,
                "live_status": live_status,
            }
        )

    normalized_rows.sort(key=lambda item: int(item["round"]))

    if not normalized_rows:
        return {
            "team": str(team_name or "").strip(),
            "available": False,
            "rounds": [],
            "last_5": [],
            "average_last_5": None,
            "best_round": None,
            "worst_round": None,
            "position_trend": "unknown",
            "message": "Andamento non ancora disponibile.",
        }

    last_5 = normalized_rows[-5:]
    average_last_5 = round(
        sum(float(item["score"]) for item in last_5) / float(len(last_5)),
        2,
    )

    best_round = max(normalized_rows, key=lambda item: (float(item["score"]), -int(item["round"])))
    worst_round = min(normalized_rows, key=lambda item: (float(item["score"]), int(item["round"])))

    positions = [item for item in last_5 if isinstance(item.get("position"), int)]
    if len(positions) < 2:
        position_trend = "unknown"
    else:
        first_pos = int(positions[0]["position"])
        last_pos = int(positions[-1]["position"])
        if last_pos < first_pos:
            position_trend = "up"
        elif last_pos > first_pos:
            position_trend = "down"
        else:
            position_trend = "stable"

    message = (
        f"Storico disponibile su {len(normalized_rows)} giornate."
        if len(normalized_rows) > 1
        else "Storico parziale disponibile su una giornata."
    )
    if position_trend == "unknown":
        message += " Trend posizione non disponibile."

    return {
        "team": str(team_name or "").strip(),
        "available": True,
        "rounds": normalized_rows,
        "last_5": last_5,
        "average_last_5": average_last_5,
        "best_round": {
            "round": int(best_round["round"]),
            "score": round(float(best_round["score"]), 2),
        },
        "worst_round": {
            "round": int(worst_round["round"]),
            "score": round(float(worst_round["score"]), 2),
        },
        "position_trend": position_trend,
        "message": message,
    }
