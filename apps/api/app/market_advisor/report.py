from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Mapping

from .io import write_csv_rows


ROLE_RANKING_HEADERS = (
    "Giocatore",
    "Squadra",
    "RuoloMantra",
    "RealScore",
    "PotentialScore",
    "CreditEfficiencyScore",
    "MarketScoreFinal",
    "Tier",
    "PrezzoAttuale",
    "PrezzoIniziale",
)

SQUAD_AUDIT_HEADERS = (
    "Team",
    "Giocatore",
    "Squadra",
    "RuoloMantra",
    "Reparto",
    "PrezzoAcquisto",
    "PrezzoAttuale",
    "RealScore",
    "PotentialScore",
    "CreditEfficiencyScore",
    "MarketScoreFinal",
    "Tier",
)

TRANSFER_PLAN_HEADERS = (
    "plan_id",
    "step",
    "out_players",
    "in_players",
    "credits_out_total",
    "credits_in_total",
    "credits_residual_before",
    "credits_residual_after",
    "package_gain",
    "notes",
)


def _now_stamp() -> str:
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def _stringify_player_list(values: Iterable[str]) -> str:
    return " | ".join(str(v) for v in values if str(v).strip())


def _snapshot_path(base_file: Path, history_dir: Path, stamp: str) -> Path:
    return history_dir / f"{base_file.stem}_{stamp}{base_file.suffix}"


def _write_role_rankings(path: Path, role_rankings: List[Mapping[str, object]]) -> None:
    rows = []
    for player in role_rankings:
        rows.append(
            {
                "Giocatore": player.get("name", ""),
                "Squadra": player.get("club", ""),
                "RuoloMantra": player.get("mantra_role_best", ""),
                "RealScore": player.get("RealScore", 0),
                "PotentialScore": player.get("PotentialScore", 0),
                "CreditEfficiencyScore": player.get("CreditEfficiencyScore", 0),
                "MarketScoreFinal": player.get("MarketScoreFinal", 0),
                "Tier": player.get("Tier", ""),
                "PrezzoAttuale": player.get("prezzo_attuale", 0),
                "PrezzoIniziale": player.get("prezzo_iniziale", 0),
            }
        )
    write_csv_rows(path, ROLE_RANKING_HEADERS, rows)


def _write_squad_audit(path: Path, team_name: str, squad_rows: List[Mapping[str, object]]) -> None:
    rows = []
    for player in squad_rows:
        rows.append(
            {
                "Team": team_name,
                "Giocatore": player.get("name", ""),
                "Squadra": player.get("club", ""),
                "RuoloMantra": player.get("mantra_role_best", ""),
                "Reparto": player.get("reparto", ""),
                "PrezzoAcquisto": player.get("prezzo_acquisto", 0),
                "PrezzoAttuale": player.get("prezzo_attuale_rosa", 0),
                "RealScore": player.get("RealScore", 0),
                "PotentialScore": player.get("PotentialScore", 0),
                "CreditEfficiencyScore": player.get("CreditEfficiencyScore", 0),
                "MarketScoreFinal": player.get("MarketScoreFinal", 0),
                "Tier": player.get("Tier", ""),
            }
        )
    write_csv_rows(path, SQUAD_AUDIT_HEADERS, rows)


def _write_transfer_plans(path: Path, plans: List[Mapping[str, object]]) -> None:
    rows = []
    for plan in plans:
        rows.append(
            {
                "plan_id": plan.get("plan_id", ""),
                "step": plan.get("step", 1),
                "out_players": _stringify_player_list(plan.get("out_players", [])),
                "in_players": _stringify_player_list(plan.get("in_players", [])),
                "credits_out_total": plan.get("credits_out_total", 0),
                "credits_in_total": plan.get("credits_in_total", 0),
                "credits_residual_before": plan.get("credits_residual_before", 0),
                "credits_residual_after": plan.get("credits_residual_after", 0),
                "package_gain": plan.get("package_gain", 0),
                "notes": plan.get("notes", ""),
            }
        )
    write_csv_rows(path, TRANSFER_PLAN_HEADERS, rows)


def write_market_advisor_reports(
    *,
    outdir: Path,
    team_name: str,
    role_rankings: List[Mapping[str, object]],
    squad_audit_rows: List[Mapping[str, object]],
    plans: List[Mapping[str, object]],
    snapshot: bool = False,
) -> Dict[str, str]:
    outdir.mkdir(parents=True, exist_ok=True)

    role_path = outdir / "role_rankings_market.csv"
    squad_path = outdir / "squad_audit_market.csv"
    transfer_path = outdir / "transfer_plan_market.csv"

    _write_role_rankings(role_path, role_rankings)
    _write_squad_audit(squad_path, team_name, squad_audit_rows)
    _write_transfer_plans(transfer_path, plans)

    written = {
        "role_rankings": str(role_path),
        "squad_audit": str(squad_path),
        "transfer_plan": str(transfer_path),
    }

    if snapshot:
        stamp = _now_stamp()
        history_dir = outdir / "history"
        history_dir.mkdir(parents=True, exist_ok=True)

        for path in (role_path, squad_path, transfer_path):
            snap_path = _snapshot_path(path, history_dir, stamp)
            snap_path.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")

        written["history_dir"] = str(history_dir)

    return written
