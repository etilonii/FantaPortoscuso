import csv
from pathlib import Path
from typing import Dict, List


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
ROSE_PATH = DATA_DIR / "rose_fantaportoscuso.csv"
TIERS_PATH = DATA_DIR / "player_tiers.csv"
REPORTS_DIR = DATA_DIR / "reports"
OUT_RANKING = REPORTS_DIR / "team_strength_ranking.csv"
OUT_PLAYERS = REPORTS_DIR / "team_strength_players.csv"
OUT_STARTING_RANKING = REPORTS_DIR / "team_starting_strength_ranking.csv"
OUT_STARTING_XI = REPORTS_DIR / "team_starting_xi.csv"


def normalize_name(value: str) -> str:
    raw = (value or "").strip()
    if raw.endswith("*"):
        raw = raw[:-1].strip()
    return "".join(ch for ch in raw.lower() if ch.isalnum())


def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = []
        for row in reader:
            cleaned = {}
            for key, value in row.items():
                if key is None:
                    continue
                cleaned[key.strip().lstrip("\ufeff")] = value
            rows.append(cleaned)
        return rows


def to_float(value: str, default: float = 0.0) -> float:
    try:
        return float(str(value).replace(",", "."))
    except Exception:
        return default


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def player_force(score_auto: float, weight: float, prezzo_attuale: float, has_tier: bool) -> tuple[float, str]:
    if has_tier:
        # Tier list has priority; keep result on a 0..100 scale.
        force = ((score_auto * 0.75) + (weight * 0.25)) * 100.0
        return round(clamp(force, 0.0, 100.0), 2), "tier"

    # Fallback when a player is missing in tier list.
    fallback = clamp(prezzo_attuale * 1.5, 0.0, 60.0)
    return round(fallback, 2), "fallback_price"


def compute_best_starting_xi(
    role_players: Dict[str, List[Dict[str, object]]]
) -> Dict[str, object]:
    gks = sorted(role_players.get("P", []), key=lambda p: float(p["force"]), reverse=True)
    defs = sorted(role_players.get("D", []), key=lambda p: float(p["force"]), reverse=True)
    mids = sorted(role_players.get("C", []), key=lambda p: float(p["force"]), reverse=True)
    atts = sorted(role_players.get("A", []), key=lambda p: float(p["force"]), reverse=True)

    if not gks:
        return {
            "force": 0.0,
            "module": "",
            "gk": "",
            "defs": [],
            "mids": [],
            "atts": [],
        }

    best: Dict[str, object] | None = None
    for d in (3, 4, 5):
        for c in (3, 4, 5):
            a = 10 - d - c
            if a < 1 or a > 3:
                continue
            if len(defs) < d or len(mids) < c or len(atts) < a:
                continue

            sel_gk = gks[0]
            sel_defs = defs[:d]
            sel_mids = mids[:c]
            sel_atts = atts[:a]
            total = (
                float(sel_gk["force"])
                + sum(float(x["force"]) for x in sel_defs)
                + sum(float(x["force"]) for x in sel_mids)
                + sum(float(x["force"]) for x in sel_atts)
            )
            row = {
                "force": round(total, 2),
                "module": f"{d}-{c}-{a}",
                "gk": str(sel_gk["name"]),
                "defs": [str(x["name"]) for x in sel_defs],
                "mids": [str(x["name"]) for x in sel_mids],
                "atts": [str(x["name"]) for x in sel_atts],
            }
            if best is None or float(row["force"]) > float(best["force"]):
                best = row

    if best is not None:
        return best

    return {
        "force": 0.0,
        "module": "",
        "gk": str(gks[0]["name"]),
        "defs": [],
        "mids": [],
        "atts": [],
    }


def main() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    rose_rows = read_csv(ROSE_PATH)
    tiers_rows = read_csv(TIERS_PATH)

    tiers_map: Dict[str, Dict[str, str]] = {}
    for row in tiers_rows:
        key = normalize_name(row.get("name", ""))
        if not key:
            continue
        tiers_map[key] = row

    team_totals: Dict[str, Dict[str, float]] = {}
    team_role_players: Dict[str, Dict[str, List[Dict[str, object]]]] = {}
    player_rows_out: List[Dict[str, str]] = []

    for row in rose_rows:
        team = (row.get("Team") or "").strip()
        name = (row.get("Giocatore") or "").strip()
        role = (row.get("Ruolo") or "").strip().upper()
        club = (row.get("Squadra") or "").strip()
        prezzo_attuale = to_float(row.get("PrezzoAttuale", "0"), 0.0)
        if not team or not name:
            continue

        key = normalize_name(name)
        tier_row = tiers_map.get(key)

        tier = (tier_row or {}).get("tier", "low")
        weight = to_float((tier_row or {}).get("weight", "0"), 0.0)
        score_auto = to_float((tier_row or {}).get("score_auto", "0"), 0.0)
        force, source = player_force(
            score_auto=score_auto,
            weight=weight,
            prezzo_attuale=prezzo_attuale,
            has_tier=tier_row is not None,
        )

        bucket = team_totals.setdefault(
            team,
            {
                "team_force": 0.0,
                "players": 0.0,
                "top_count": 0.0,
                "semitop_count": 0.0,
                "starter_count": 0.0,
                "scommessa_count": 0.0,
                "low_count": 0.0,
                "missing_tier_count": 0.0,
            },
        )
        bucket["team_force"] += force
        bucket["players"] += 1
        if tier == "top":
            bucket["top_count"] += 1
        elif tier == "semitop":
            bucket["semitop_count"] += 1
        elif tier == "starter":
            bucket["starter_count"] += 1
        elif tier == "scommessa":
            bucket["scommessa_count"] += 1
        else:
            bucket["low_count"] += 1
        if tier_row is None:
            bucket["missing_tier_count"] += 1

        player_rows_out.append(
            {
                "Team": team,
                "Giocatore": name,
                "Ruolo": role,
                "Squadra": club,
                "Tier": tier,
                "Weight": f"{weight:.3f}",
                "ScoreAuto": f"{score_auto:.3f}",
                "PrezzoAttuale": f"{prezzo_attuale:.1f}",
                "ForzaGiocatore": f"{force:.2f}",
                "Source": source,
            }
        )

        team_role_players.setdefault(team, {"P": [], "D": [], "C": [], "A": []})
        if role in ("P", "D", "C", "A"):
            team_role_players[team][role].append(
                {
                    "name": name,
                    "force": force,
                }
            )

    team_starting: Dict[str, Dict[str, object]] = {}
    for team, role_players in team_role_players.items():
        team_starting[team] = compute_best_starting_xi(role_players)

    ranking_rows: List[Dict[str, str]] = []
    for team, vals in team_totals.items():
        players = int(vals["players"])
        team_force = float(vals["team_force"])
        avg_force = team_force / players if players else 0.0
        team_xi = team_starting.get(team, {})
        ranking_rows.append(
            {
                "Team": team,
                "ForzaSquadra": f"{team_force:.2f}",
                "ForzaMediaGiocatore": f"{avg_force:.2f}",
                "ForzaTitolari": f"{float(team_xi.get('force', 0.0)):.2f}",
                "ModuloMigliore": str(team_xi.get("module", "")),
                "Giocatori": str(players),
                "Top": str(int(vals["top_count"])),
                "SemiTop": str(int(vals["semitop_count"])),
                "Starter": str(int(vals["starter_count"])),
                "Scommessa": str(int(vals["scommessa_count"])),
                "Low": str(int(vals["low_count"])),
                "MissingTier": str(int(vals["missing_tier_count"])),
            }
        )

    ranking_rows.sort(
        key=lambda row: (
            -to_float(row["ForzaSquadra"], 0.0),
            row["Team"].lower(),
        )
    )

    for idx, row in enumerate(ranking_rows, start=1):
        row["Pos"] = str(idx)

    player_rows_out.sort(
        key=lambda row: (
            int(
                next(
                    (
                        rr["Pos"]
                        for rr in ranking_rows
                        if rr["Team"].lower() == row["Team"].lower()
                    ),
                    "999",
                )
            ),
            row["Team"].lower(),
            -to_float(row["ForzaGiocatore"], 0.0),
            row["Giocatore"].lower(),
        )
    )

    with OUT_RANKING.open("w", encoding="utf-8", newline="") as handle:
        fields = [
            "Pos",
            "Team",
            "ForzaSquadra",
            "ForzaMediaGiocatore",
            "ForzaTitolari",
            "ModuloMigliore",
            "Giocatori",
            "Top",
            "SemiTop",
            "Starter",
            "Scommessa",
            "Low",
            "MissingTier",
        ]
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(ranking_rows)

    with OUT_PLAYERS.open("w", encoding="utf-8", newline="") as handle:
        fields = [
            "Team",
            "Giocatore",
            "Ruolo",
            "Squadra",
            "Tier",
            "Weight",
            "ScoreAuto",
            "PrezzoAttuale",
            "ForzaGiocatore",
            "Source",
        ]
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(player_rows_out)

    starting_ranking_rows: List[Dict[str, str]] = []
    for team, info in team_starting.items():
        starting_ranking_rows.append(
            {
                "Team": team,
                "ForzaTitolari": f"{float(info.get('force', 0.0)):.2f}",
                "ModuloMigliore": str(info.get("module", "")),
                "Portiere": str(info.get("gk", "")),
                "Difensori": "; ".join(info.get("defs", [])),
                "Centrocampisti": "; ".join(info.get("mids", [])),
                "Attaccanti": "; ".join(info.get("atts", [])),
            }
        )

    starting_ranking_rows.sort(
        key=lambda row: (
            -to_float(row["ForzaTitolari"], 0.0),
            row["Team"].lower(),
        )
    )
    for idx, row in enumerate(starting_ranking_rows, start=1):
        row["Pos"] = str(idx)

    with OUT_STARTING_RANKING.open("w", encoding="utf-8", newline="") as handle:
        fields = [
            "Pos",
            "Team",
            "ForzaTitolari",
            "ModuloMigliore",
        ]
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(starting_ranking_rows)

    with OUT_STARTING_XI.open("w", encoding="utf-8", newline="") as handle:
        fields = [
            "Pos",
            "Team",
            "ForzaTitolari",
            "ModuloMigliore",
            "Portiere",
            "Difensori",
            "Centrocampisti",
            "Attaccanti",
        ]
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(starting_ranking_rows)

    print(f"Ranking written: {OUT_RANKING}")
    print(f"Players detail written: {OUT_PLAYERS}")
    print(f"Starting XI ranking written: {OUT_STARTING_RANKING}")
    print(f"Starting XI detail written: {OUT_STARTING_XI}")
    print(f"Teams ranked: {len(ranking_rows)}")


if __name__ == "__main__":
    main()
