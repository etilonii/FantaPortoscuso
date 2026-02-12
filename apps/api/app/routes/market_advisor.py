from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from apps.api.app.market_advisor import run_market_advisor


router = APIRouter(prefix="/market", tags=["market-advisor"])


@router.get("/advisor")
def market_advisor(
    team: str = Query(..., min_length=1, alias="team"),
    window: str = Query(..., min_length=5, alias="window"),
    initial_budget: float = Query(default=250.0, ge=0.0),
    games_min: int = Query(default=10, ge=0, le=50),
    top_n: int = Query(default=25, ge=1, le=200),
    outdir: str = Query(default="data/reports"),
    snapshot: bool = Query(default=False),
    credits_cost_source: str = Query(default="current", pattern="^(current|initial)$"),
    sell_valuation: str = Query(default="current", pattern="^(current|paid)$"),
    min_delta: float = Query(default=4.0),
    beam_width: int = Query(default=30, ge=1, le=500),
    max_k: int | None = Query(default=None, ge=1, le=10),
    out_candidate_limit: int = Query(default=20, ge=1, le=200),
    in_candidate_limit: int = Query(default=40, ge=1, le=500),
    top_plans: int = Query(default=3, ge=1, le=20),
    enforce_initial_bands: bool = Query(default=False),
):
    try:
        return run_market_advisor(
            team_name=team,
            market_window=window,
            initial_budget=initial_budget,
            games_min=games_min,
            top_n=top_n,
            outdir=outdir,
            snapshot=snapshot,
            credits_cost_source=credits_cost_source,
            sell_valuation=sell_valuation,
            min_delta=min_delta,
            beam_width=beam_width,
            max_k=max_k,
            out_candidate_limit=out_candidate_limit,
            in_candidate_limit=in_candidate_limit,
            top_plans=top_plans,
            enforce_initial_bands=enforce_initial_bands,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
