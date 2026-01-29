import csv
from pathlib import Path

import pandas as pd

from apps.api.app.engine import market_engine as me

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
REPORT = DATA / "reports" / "top_semitop_report.txt"
GK_MANUAL = DATA / "db" / "manual_tiers_goalkeepers.csv"

ps = pd.read_csv(DATA / "db" / "player_stats.csv")
qs = pd.read_csv(DATA / "db" / "quotazioni_master.csv")
teams_df = pd.read_csv(DATA / "db" / "teams.csv")
fixtures = pd.read_csv(DATA / "db" / "fixtures.csv").to_dict(orient="records")
current_round = 23
meta = DATA / "db" / "league_meta.csv"
if meta.exists():
    try:
        current_round = int(pd.read_csv(meta).iloc[0]["currentRound"])
    except Exception:
        pass

teams_data = {}
for _, r in teams_df.iterrows():
    teams_data[r["name"]] = {
        "PPG_S": float(r.get("PPG_S", 0) or 0),
        "GFpg_S": float(r.get("GFpg_S", 0) or 0),
        "GApg_S": float(r.get("GApg_S", 0) or 0),
        "PPG_R8": float(r.get("PPG_R8", 0) or 0),
        "GFpg_R8": float(r.get("GFpg_R8", 0) or 0),
        "GApg_R8": float(r.get("GApg_R8", 0) or 0),
        "MoodTeam": float(r.get("MoodTeam", 0.5) or 0.5),
        "CoachStyle_P": float(r.get("CoachStyle_P", 0.5) or 0.5),
        "CoachStyle_D": float(r.get("CoachStyle_D", 0.5) or 0.5),
        "CoachStyle_C": float(r.get("CoachStyle_C", 0.5) or 0.5),
        "CoachStyle_A": float(r.get("CoachStyle_A", 0.5) or 0.5),
        "CoachStability": float(r.get("CoachStability", 0.5) or 0.5),
        "CoachBoost": float(r.get("CoachBoost", 0.5) or 0.5),
    }

players = ps.merge(qs, left_on="Giocatore", right_on="nome", how="left")
pool = []
for r in players.itertuples(index=False):
    name = getattr(r, "Giocatore", None)
    if not isinstance(name, str) or not name:
        continue
    club = getattr(r, "club", "") or getattr(r, "Squadra", "") or ""
    role = str(getattr(r, "R", "") or "").upper()
    if not club or club not in teams_data:
        continue
    p = {
        "nome": name,
        "ruolo_base": role,
        "club": club,
        "QA": float(getattr(r, "QA", 0) or 0),
        "PV_S": float(getattr(r, "PV_S", 0) or 0),
        "PV_R8": float(getattr(r, "PV_R8", 0) or 0),
        "PT_S": float(getattr(r, "PT_S", 0) or 0),
        "PT_R8": float(getattr(r, "PT_R8", 0) or 0),
        "MIN_S": float(getattr(r, "MIN_S", 0) or 0),
        "MIN_R8": float(getattr(r, "MIN_R8", 0) or 0),
        "G_S": float(getattr(r, "G_S", 0) or 0),
        "G_R8": float(getattr(r, "G_R8", 0) or 0),
        "A_S": float(getattr(r, "A_S", 0) or 0),
        "A_R8": float(getattr(r, "A_R8", 0) or 0),
        "xG_S": float(getattr(r, "xG_S", 0) or 0),
        "xG_R8": float(getattr(r, "xG_R8", 0) or 0),
        "xA_S": float(getattr(r, "xA_S", 0) or 0),
        "xA_R8": float(getattr(r, "xA_R8", 0) or 0),
        "AMM_S": float(getattr(r, "AMM_S", 0) or 0),
        "AMM_R8": float(getattr(r, "AMM_R8", 0) or 0),
        "ESP_S": float(getattr(r, "ESP_S", 0) or 0),
        "ESP_R8": float(getattr(r, "ESP_R8", 0) or 0),
        "AUTOGOL_S": float(getattr(r, "AUTOGOL_S", 0) or 0),
        "AUTOGOL_R8": float(getattr(r, "AUTOGOL_R8", 0) or 0),
        "RIGSEG_S": float(getattr(r, "RIGSEG_S", 0) or 0),
        "RIGSEG_R8": float(getattr(r, "RIGSEG_R8", 0) or 0),
        "RIGSBAGL_S": float(getattr(r, "RIGSBAGL_S", 0) or 0),
        "RIGSBAGL_R8": float(getattr(r, "RIGSBAGL_R8", 0) or 0),
        "GOLS_S": float(getattr(r, "GOLS_S", 0) or 0),
        "GOLS_R8": float(getattr(r, "GOLS_R8", 0) or 0),
        "RIGPAR_S": float(getattr(r, "RIGPAR_S", 0) or 0),
        "RIGPAR_R8": float(getattr(r, "RIGPAR_R8", 0) or 0),
        "CS_S": float(getattr(r, "CS_S", 0) or 0),
        "CS_R8": float(getattr(r, "CS_R8", 0) or 0),
        "PKRole": float(getattr(r, "PKRole", 0) or 0),
    }
    pool.append(p)

values = []
for p in pool:
    tit = me.titolarita(p, pool)
    val = me.value_season(p, pool, teams_data, fixtures, current_round)
    values.append((p["nome"], p["ruolo_base"], p["club"], val, tit))

manual_gk = {}
if GK_MANUAL.exists():
    with GK_MANUAL.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            name = row.get("Giocatore", "").strip()
            tier = row.get("Tier", "").strip()
            if name and tier:
                manual_gk[name] = tier

from collections import defaultdict

role_vals = defaultdict(list)
for name, role, club, val, tit in values:
    if role == "P":
        tier = manual_gk.get(name, "Panchinaro")
        role_vals[role].append((tier, name, club, val, tit))
        continue
    if tit < 0.65:
        continue
    role_vals[role].append(("AUTO", name, club, val, tit))

out = {}
for role, items in role_vals.items():
    if role == "P":
        out[role] = {
            "Top": [x for x in items if x[0] == "Top"],
            "SemiTop": [x for x in items if x[0] == "SemiTop"],
            "Titolare": [x for x in items if x[0] == "Titolare"],
            "Panchinaro": [x for x in items if x[0] == "Panchinaro"],
        }
        continue
    dedup = {}
    for _, name, club, val, tit in items:
        if name not in dedup or val > dedup[name][1]:
            dedup[name] = (club, val, tit)
    dedup_items = [(n,) + dedup[n] for n in dedup]
    dedup_items.sort(key=lambda x: x[2], reverse=True)
    n = len(dedup_items)
    if n == 0:
        continue
    top_cut = max(1, round(n * 0.15))
    semi_cut = max(top_cut + 1, round(n * 0.35))
    out[role] = {
        "n": n,
        "top_cut": top_cut,
        "semi_cut": semi_cut,
        "top": dedup_items[:top_cut],
        "semi": dedup_items[top_cut:semi_cut],
    }

REPORT.parent.mkdir(parents=True, exist_ok=True)
lines = []
lines.append("Filtro titolarita >= 0.65; duplicati rimossi. Portieri da manual tiers.")
lines.append("")

if "P" in out:
    lines.append("== P (manual tiers) ==")
    for label in ["Top", "SemiTop", "Titolare", "Panchinaro"]:
        lines.append(f"{label}:")
        for _, name, club, val, tit in out["P"][label]:
            lines.append(f"- {name} ({club}) | {label}")
        lines.append("")

for role in ["D", "C", "A"]:
    if role not in out:
        continue
    lines.append(
        "== {0} (n={1}, top<= {2}, semi<= {3}) ==".format(
            role, out[role]["n"], out[role]["top_cut"], out[role]["semi_cut"]
        )
    )
    lines.append("TOP:")
    for name, club, val, tit in out[role]["top"]:
        lines.append("- {0} ({1}) | {2:.2f} | Tit {3:.2f}".format(name, club, val, tit))
    lines.append("SEMI-TOP:")
    for name, club, val, tit in out[role]["semi"]:
        lines.append("- {0} ({1}) | {2:.2f} | Tit {3:.2f}".format(name, club, val, tit))
    lines.append("")

REPORT.write_text("\n".join(lines), encoding="utf-8")
print(str(REPORT))
