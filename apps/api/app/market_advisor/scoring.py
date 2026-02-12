from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Mapping


SHRINKAGE_K = 10.0
SHRINKAGE_K_BY_BUCKET: Dict[str, float] = {
    "Por": 10.0,
    "Dif": 10.0,
    "Cen": 9.0,
    "Att": 6.0,
}


REAL_WEIGHTS: Dict[str, Dict[str, float]] = {
    "Por": {
        "fm": 0.34,
        "mv": 0.16,
        "clean_pg": 0.18,
        "rigori_parati_pg": 0.14,
        "concede_pg": 0.18,
        "team_context": 0.08,
    },
    "Dif": {
        "fm": 0.28,
        "mv": 0.18,
        "clean_pg": 0.14,
        "gol_pg": 0.14,
        "assist_pg": 0.08,
        "decisive_pg": 0.06,
        "discipline_pg": 0.10,
        "team_context": 0.08,
    },
    "Cen": {
        "fm": 0.28,
        "mv": 0.16,
        "gol_pg": 0.20,
        "assist_pg": 0.16,
        "decisive_pg": 0.10,
        "discipline_pg": 0.08,
        "team_context": 0.08,
    },
    "Att": {
        "fm": 0.28,
        "mv": 0.14,
        "gol_pg": 0.30,
        "assist_pg": 0.10,
        "decisive_pg": 0.10,
        "discipline_pg": 0.06,
        "team_context": 0.10,
    },
}

POTENTIAL_WEIGHTS: Dict[str, Dict[str, float]] = {
    "Por": {
        "fm": 0.20,
        "clean_pg": 0.20,
        "rigori_parati_pg": 0.12,
        "availability": 0.18,
        "upside_signal": 0.24,
        "team_context": 0.08,
    },
    "Dif": {
        "fm": 0.18,
        "gol_pg": 0.10,
        "assist_pg": 0.12,
        "decisive_pg": 0.10,
        "availability": 0.18,
        "upside_signal": 0.24,
        "team_context": 0.10,
    },
    "Cen": {
        "fm": 0.16,
        "gol_pg": 0.16,
        "assist_pg": 0.16,
        "decisive_pg": 0.10,
        "availability": 0.14,
        "upside_signal": 0.22,
        "team_context": 0.12,
    },
    "Att": {
        "fm": 0.16,
        "gol_pg": 0.24,
        "assist_pg": 0.12,
        "decisive_pg": 0.12,
        "availability": 0.12,
        "upside_signal": 0.18,
        "team_context": 0.14,
    },
}

NEGATIVE_METRICS = {"discipline_index", "discipline_pg", "concede_pg"}


def _role_bucket(player: Mapping[str, object]) -> str:
    role = str(player.get("mantra_role_best") or "")
    if role == "Por":
        return "Por"
    if role in {"Dc", "Dd", "Ds", "B"}:
        return "Dif"
    if role in {"E", "M", "C", "T", "W"}:
        return "Cen"
    if role in {"A", "Pc"}:
        return "Att"
    return "Cen"


def _percentile_rank(values: List[float], value: float) -> float:
    n = len(values)
    if n <= 1:
        return 50.0
    less = 0
    equal = 0
    for current in values:
        if current < value:
            less += 1
        elif current == value:
            equal += 1
    rank = (less + max(0, equal - 1) * 0.5) / float(n - 1)
    return max(0.0, min(100.0, rank * 100.0))


def percentile_by_role(
    players: List[Dict[str, object]],
    role_bucket: str,
    col: str,
    invert: bool = False,
) -> Dict[str, float]:
    values: List[float] = []
    role_players: List[Dict[str, object]] = []
    for player in players:
        if _role_bucket(player) != role_bucket:
            continue
        features = dict(player.get("features") or {})
        if col not in features:
            continue
        value = float(features.get(col) or 0.0)
        values.append(value)
        role_players.append(player)
    if not values:
        return {}
    out: Dict[str, float] = {}
    for player in role_players:
        key = str(player.get("name_key") or "")
        value = float((player.get("features") or {}).get(col) or 0.0)
        p = _percentile_rank(values, value)
        out[key] = 100.0 - p if invert else p
    return out


def _role_means(players: List[Dict[str, object]], metrics: List[str]) -> Dict[str, Dict[str, float]]:
    sums: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    cnts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for player in players:
        bucket = _role_bucket(player)
        features = dict(player.get("features") or {})
        for metric in metrics:
            if metric not in features:
                continue
            sums[bucket][metric] += float(features.get(metric) or 0.0)
            cnts[bucket][metric] += 1
    out: Dict[str, Dict[str, float]] = defaultdict(dict)
    for bucket, metrics_map in sums.items():
        for metric, total in metrics_map.items():
            count = max(1, cnts[bucket][metric])
            out[bucket][metric] = total / count
    return out


def _apply_shrinkage(players: List[Dict[str, object]]) -> None:
    metrics = sorted(
        {
            *{m for group in REAL_WEIGHTS.values() for m in group},
            *{m for group in POTENTIAL_WEIGHTS.values() for m in group},
        }
    )
    means = _role_means(players, metrics)
    for player in players:
        features = dict(player.get("features") or {})
        bucket = _role_bucket(player)
        games = float(features.get("games") or 0.0)
        k = float(SHRINKAGE_K_BY_BUCKET.get(bucket, SHRINKAGE_K))
        shrink_factor = games / (games + k) if games >= 0 else 0.0
        for metric in metrics:
            if metric not in features:
                continue
            if metric in {"availability", "upside_signal", "team_context"}:
                continue
            base = float(features.get(metric) or 0.0)
            mean = float(means.get(bucket, {}).get(metric, base))
            features[metric] = (base * shrink_factor) + (mean * (1.0 - shrink_factor))
        player["features"] = features


def _weighted_score(
    key: str,
    weights: Dict[str, float],
    percentile_maps: Dict[str, Dict[str, float]],
) -> float:
    total_w = 0.0
    total = 0.0
    for metric, weight in weights.items():
        p = percentile_maps.get(metric, {}).get(key)
        if p is None:
            continue
        total_w += weight
        total += weight * p
    if total_w <= 0:
        return 0.0
    return max(0.0, min(100.0, total / total_w))


def _attacker_form_burst_bonus(player: Mapping[str, object]) -> float:
    """
    Small-sample in-form bonus for forwards.
    Goal: reward true hot streaks (e.g. 3 goals in 4 matches) without
    exploding one-match noise.
    """
    features = dict(player.get("features") or {})
    stats = dict(player.get("stats") or {})
    games = float(stats.get("Partite") or features.get("games") or 0.0)
    if games <= 0.0 or games > 8.0:
        return 0.0

    price = float(player.get("prezzo_attuale") or 0.0)
    if price < 10.0:
        return 0.0

    gol = float(stats.get("Gol") or 0.0)
    gv = float(stats.get("GolVittoria") or 0.0)
    gp = float(stats.get("GolPareggio") or 0.0)
    gol_pg = gol / max(1.0, games)
    decisive_pg = (gv + (0.5 * gp)) / max(1.0, games)
    team_ctx = max(0.0, min(100.0, float(features.get("team_context") or 50.0))) / 100.0

    burst = max(0.0, gol_pg - 0.45) * 8.0
    burst += max(0.0, decisive_pg - 0.10) * 4.0
    if burst <= 0.0:
        return 0.0

    reliability = max(0.0, min(1.0, games / 4.0))
    context_factor = 0.4 + (0.6 * team_ctx)
    bonus = burst * reliability * context_factor
    return max(0.0, min(4.5, bonus))


def compute_scores(players: List[Dict[str, object]], in_cost_source: str = "current") -> None:
    if not players:
        return
    _apply_shrinkage(players)

    metrics = sorted(
        {
            *{m for group in REAL_WEIGHTS.values() for m in group},
            *{m for group in POTENTIAL_WEIGHTS.values() for m in group},
        }
    )
    percentile_maps: Dict[str, Dict[str, float]] = {}
    for bucket in ("Por", "Dif", "Cen", "Att"):
        for metric in metrics:
            metric_map = percentile_by_role(
                players=players,
                role_bucket=bucket,
                col=metric,
                invert=metric in NEGATIVE_METRICS,
            )
            if metric not in percentile_maps:
                percentile_maps[metric] = {}
            percentile_maps[metric].update(metric_map)

    raw_eff_by_bucket: Dict[str, List[float]] = defaultdict(list)
    for player in players:
        key = str(player.get("name_key") or "")
        bucket = _role_bucket(player)
        real = _weighted_score(key, REAL_WEIGHTS[bucket], percentile_maps)
        potential = _weighted_score(key, POTENTIAL_WEIGHTS[bucket], percentile_maps)
        player["RealScore"] = round(real, 2)
        player["PotentialScore"] = round(potential, 2)
        if str(in_cost_source).lower() == "initial":
            cost = float(player.get("prezzo_iniziale") or 0.0)
        else:
            cost = float(player.get("prezzo_attuale") or 0.0)
        cost = max(1.0, cost)
        raw_eff = ((0.6 * real) + (0.4 * potential)) / cost
        player["_raw_eff"] = raw_eff
        raw_eff_by_bucket[bucket].append(raw_eff)

    for player in players:
        bucket = _role_bucket(player)
        values = raw_eff_by_bucket.get(bucket, [])
        p = _percentile_rank(values, float(player.get("_raw_eff") or 0.0)) if values else 50.0
        player["CreditEfficiencyScore"] = round(p, 2)
        final_score = (
            (0.50 * float(player.get("RealScore") or 0.0))
            + (0.30 * float(player.get("PotentialScore") or 0.0))
            + (0.20 * float(player.get("CreditEfficiencyScore") or 0.0))
        )
        if bucket == "Att":
            final_score += _attacker_form_burst_bonus(player)
        player["MarketScoreFinal"] = round(max(0.0, min(100.0, final_score)), 2)
        if "_raw_eff" in player:
            player.pop("_raw_eff")
