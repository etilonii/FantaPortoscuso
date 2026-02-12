from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.api.app.market_advisor import run_market_advisor  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build Market Advisor reports for FantaPortoscuso")
    parser.add_argument("--team-name", required=True, help="Team name as in data/rose_fantaportoscuso.csv")
    parser.add_argument("--market-window", required=True, help="Window key, e.g. 03-02-2026_06-02-2026")
    parser.add_argument("--initial-budget", type=float, default=250.0, help="Fallback only")
    parser.add_argument("--games-min", type=int, default=10)
    parser.add_argument("--top-n", type=int, default=25)
    parser.add_argument("--outdir", default="data/reports")
    parser.add_argument("--snapshot", action="store_true")
    parser.add_argument("--credits-cost-source", choices=["current", "initial"], default="current")
    parser.add_argument("--sell-valuation", choices=["current", "paid"], default="current")
    parser.add_argument("--min-delta", type=float, default=4.0)
    parser.add_argument("--beam-width", type=int, default=30)
    parser.add_argument("--max-k", type=int, default=None)
    parser.add_argument("--out-candidate-limit", type=int, default=20)
    parser.add_argument("--in-candidate-limit", type=int, default=40)
    parser.add_argument("--top-plans", type=int, default=3)
    parser.add_argument("--enforce-initial-bands", action="store_true")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    payload = run_market_advisor(
        team_name=args.team_name,
        market_window=args.market_window,
        initial_budget=args.initial_budget,
        games_min=args.games_min,
        top_n=args.top_n,
        outdir=args.outdir,
        snapshot=args.snapshot,
        credits_cost_source=args.credits_cost_source,
        sell_valuation=args.sell_valuation,
        min_delta=args.min_delta,
        beam_width=args.beam_width,
        max_k=args.max_k,
        out_candidate_limit=args.out_candidate_limit,
        in_candidate_limit=args.in_candidate_limit,
        top_plans=args.top_plans,
        enforce_initial_bands=args.enforce_initial_bands,
    )

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
