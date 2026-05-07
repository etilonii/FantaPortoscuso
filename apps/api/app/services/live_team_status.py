from __future__ import annotations

from typing import Dict, List, Optional


FORMATION_ROLE_FIELDS = (
    ("portiere", "P"),
    ("difensori", "D"),
    ("centrocampisti", "C"),
    ("attaccanti", "A"),
)


def _normalize_name(value: object) -> str:
    return "".join(ch.lower() for ch in str(value or "").strip() if ch.isalnum())


def _safe_number(value: object) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _unique_names(values: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for value in values:
        name = str(value or "").strip()
        key = _normalize_name(name)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(name)
    return out


def _original_starter_names(item: Dict[str, object]) -> List[str]:
    original = item.get("original_lineup") if isinstance(item.get("original_lineup"), dict) else {}
    names: List[str] = []
    goalkeeper = str(original.get("portiere") or item.get("portiere") or "").strip()
    if goalkeeper:
        names.append(goalkeeper)
    for field, _role in FORMATION_ROLE_FIELDS[1:]:
        values = original.get(field, item.get(field))
        if isinstance(values, list):
            names.extend(str(value or "").strip() for value in values if str(value or "").strip())
    return _unique_names(names)


def _effective_players(item: Dict[str, object]) -> List[Dict[str, str]]:
    players: List[Dict[str, str]] = []
    goalkeeper = str(item.get("portiere") or "").strip()
    if goalkeeper:
        players.append({"name": goalkeeper, "role": "P"})
    for field, role in FORMATION_ROLE_FIELDS[1:]:
        values = item.get(field)
        if not isinstance(values, list):
            continue
        for value in values:
            name = str(value or "").strip()
            if name:
                players.append({"name": name, "role": role})
    return players


def _score_is_definitive(score: object) -> bool:
    if not isinstance(score, dict):
        return False
    if bool(score.get("is_sv")) or bool(score.get("is_absent")):
        return True
    source = str(score.get("source") or "").strip().lower()
    if source not in {"manual", "appkey", "import_absent"}:
        return False
    return _safe_number(score.get("fantavote")) is not None or _safe_number(score.get("vote")) is not None


def _build_modifier_status(
    modifier_name: str,
    item: Dict[str, object],
    player_scores: Dict[str, Dict[str, object]],
    regulation: Dict[str, object],
    pending_player_keys: set[str],
) -> str:
    modifiers = regulation.get("modifiers") if isinstance(regulation, dict) else {}
    modifier_cfg = modifiers.get(modifier_name) if isinstance(modifiers, dict) else {}
    if not isinstance(modifier_cfg, dict) or not bool(modifier_cfg.get("enabled")):
        return "not_applicable"

    if modifier_name == "difesa":
        defenders = item.get("difensori") if isinstance(item.get("difensori"), list) else []
        defenders = [str(value or "").strip() for value in defenders if str(value or "").strip()]
        minimum = int(modifier_cfg.get("requires_defenders_min") or 4)
        if len(defenders) < minimum:
            return "not_applicable"
        goalkeeper = str(item.get("portiere") or "").strip()
        relevant = defenders[: max(3, minimum)]
        include_goalkeeper = bool(
            modifier_cfg.get("include_goalkeeper_vote", modifier_cfg.get("use_goalkeeper", True))
        )
        if include_goalkeeper and goalkeeper:
            relevant = [goalkeeper, *relevant]
        if not relevant:
            return "unknown"
        if any(_normalize_name(name) in pending_player_keys for name in relevant):
            return "in_progress"
        return "final"

    if modifier_name == "capitano":
        captain = str(item.get("capitano") or item.get("captain") or "").strip()
        vice = str(
            item.get("vice_capitano") or item.get("vicecaptain") or item.get("vice_captain") or ""
        ).strip()
        candidates = [name for name in [captain, vice] if name]
        if any(_score_is_definitive(player_scores.get(name)) for name in candidates):
            return "final"
        if candidates and any(_normalize_name(name) in pending_player_keys for name in candidates):
            return "in_progress"
        if _safe_number(item.get("mod_capitano")) is not None:
            return "final"
        return "unknown"

    return "unknown"


def build_live_team_status(
    item: Dict[str, object],
    *,
    regulation: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    player_scores_raw = item.get("player_scores")
    player_scores = player_scores_raw if isinstance(player_scores_raw, dict) else {}
    original_starters = _original_starter_names(item)
    effective_players = _effective_players(item)
    substitutions = item.get("substitutions") if isinstance(item.get("substitutions"), list) else []
    missing_players = [
        str(value or "").strip()
        for value in (item.get("missing_players") if isinstance(item.get("missing_players"), list) else [])
        if str(value or "").strip()
    ]
    reserves = item.get("panchina_details") if isinstance(item.get("panchina_details"), list) else []
    reserves_available = len(reserves) > 0 or bool(item.get("panchina"))

    if len(original_starters) < 11 or len(effective_players) < 11:
        return {
            "live_status": "needs_review",
            "live_status_label": "Da verificare",
            "live_status_reason": "Formazione incompleta o non leggibile",
            "pending_players": [],
            "resolved_substitutions": len(substitutions),
            "resolved_substitutions_list": substitutions,
            "pending_substitutions": len(missing_players),
            "pending_substitutions_list": missing_players,
            "modifiers_status": {"difesa": "unknown", "capitano": "unknown"},
        }

    pending_players = [
        player["name"]
        for player in effective_players
        if not _score_is_definitive(player_scores.get(player["name"]))
    ]
    pending_player_keys = {_normalize_name(name) for name in pending_players}

    modifiers_status = {
        "difesa": _build_modifier_status(
            "difesa",
            item,
            player_scores,
            regulation or {},
            pending_player_keys,
        ),
        "capitano": _build_modifier_status(
            "capitano",
            item,
            player_scores,
            regulation or {},
            pending_player_keys,
        ),
    }

    if missing_players and not reserves_available:
        return {
            "live_status": "needs_review",
            "live_status_label": "Da verificare",
            "live_status_reason": "Panchina mancante per risolvere le sostituzioni",
            "pending_players": _unique_names([*pending_players, *missing_players]),
            "resolved_substitutions": len(substitutions),
            "resolved_substitutions_list": substitutions,
            "pending_substitutions": len(missing_players),
            "pending_substitutions_list": missing_players,
            "modifiers_status": modifiers_status,
        }

    if pending_players or missing_players or "in_progress" in modifiers_status.values():
        if missing_players:
            reason = "Sostituzioni ancora pendenti"
        elif pending_players:
            reason = f"{len(_unique_names(pending_players))} giocatori rilevanti ancora pendenti"
        else:
            reason = "Modificatori non ancora determinabili"
        return {
            "live_status": "in_progress",
            "live_status_label": "In corso",
            "live_status_reason": reason,
            "pending_players": _unique_names([*pending_players, *missing_players]),
            "resolved_substitutions": len(substitutions),
            "resolved_substitutions_list": substitutions,
            "pending_substitutions": len(missing_players),
            "pending_substitutions_list": missing_players,
            "modifiers_status": modifiers_status,
        }

    if "unknown" in modifiers_status.values():
        return {
            "live_status": "needs_review",
            "live_status_label": "Da verificare",
            "live_status_reason": "Modificatori non determinabili con i dati disponibili",
            "pending_players": [],
            "resolved_substitutions": len(substitutions),
            "resolved_substitutions_list": substitutions,
            "pending_substitutions": len(missing_players),
            "pending_substitutions_list": missing_players,
            "modifiers_status": modifiers_status,
        }

    final_reason = (
        "Titolari definitivi e sostituzioni risolte"
        if substitutions
        else "Tutti gli 11 effettivi hanno voto definitivo"
    )
    return {
        "live_status": "final",
        "live_status_label": "Finale",
        "live_status_reason": final_reason,
        "pending_players": [],
        "resolved_substitutions": len(substitutions),
        "resolved_substitutions_list": substitutions,
        "pending_substitutions": 0,
        "pending_substitutions_list": [],
        "modifiers_status": modifiers_status,
    }
