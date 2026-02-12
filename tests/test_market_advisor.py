import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from apps.api.app.market_advisor.credits import load_residual_credits_map
from apps.api.app.market_advisor.roles import parse_positions_to_roles
from apps.api.app.market_advisor.rules import validate_roster
from apps.api.app.market_advisor.transfers import plan_market_campaign
from apps.api.app.utils.names import normalize_name


def _player(name: str, role: str, club: str, market: float, tier: str = "Titolare"):
    return {
        "name": name,
        "name_key": normalize_name(name),
        "club": club,
        "mantra_role_best": role,
        "MarketScoreFinal": market,
        "Tier": tier,
        "features": {
            "discipline_pg": 0.05,
            "availability": 1.0,
        },
        "in_cost": 5.0,
        "out_value": 5.0,
    }


def _valid_roster() -> list[dict]:
    players = []
    # Por 3
    for i in range(3):
        players.append(_player(f"P{i}", "Por", f"ClubP{i}", 55.0))
    # Dif 7
    dif_roles = ["Dc", "Dd", "Ds", "B", "Dc", "Dd", "Ds"]
    for i, role in enumerate(dif_roles):
        players.append(_player(f"D{i}", role, f"ClubD{i}", 55.0))
    # Cen 8
    cen_roles = ["E", "M", "C", "T", "W", "C", "M", "E"]
    for i, role in enumerate(cen_roles):
        players.append(_player(f"C{i}", role, f"ClubC{i}", 55.0))
    # Att 5
    att_roles = ["A", "Pc", "A", "Pc", "A"]
    for i, role in enumerate(att_roles):
        players.append(_player(f"A{i}", role, f"ClubA{i}", 55.0))
    return players


class MarketAdvisorTests(unittest.TestCase):
    def test_parse_positions_multi_role(self):
        roles = parse_positions_to_roles("Dc/B, E/W C T Dd")
        self.assertEqual(roles, {"Dc", "B", "E", "W", "C", "T", "Dd"})

    def test_validate_roster_and_team_cap(self):
        roster = _valid_roster()
        ok, reasons, _ = validate_roster(roster)
        self.assertTrue(ok)
        self.assertEqual(reasons, [])

        # Force 4 players from same club to trigger cap violation.
        roster[0]["club"] = "X"
        roster[1]["club"] = "X"
        roster[2]["club"] = "X"
        roster[3]["club"] = "X"
        ok2, reasons2, _ = validate_roster(roster)
        self.assertFalse(ok2)
        self.assertTrue(any("Team-cap" in r for r in reasons2))

    def test_load_residual_credits_from_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "rose_nuovo_credits.csv"
            p.write_text("Team,CreditiResidui\nAlpha,42\n", encoding="utf-8")
            with patch("apps.api.app.market_advisor.credits.credits_path", return_value=p):
                data = load_residual_credits_map()
        self.assertIn("alpha", data)
        self.assertEqual(data["alpha"], 42.0)

    def test_transfer_packages_support_2x2(self):
        roster = _valid_roster()
        # Make two defenders very weak so planner wants to sell them.
        roster[3]["MarketScoreFinal"] = 20.0
        roster[4]["MarketScoreFinal"] = 18.0

        in_candidates = [
            _player("IN_D1", "Dc", "ClubX1", 85.0, tier="Top"),
            _player("IN_D2", "Dd", "ClubX2", 83.0, tier="Top"),
            _player("IN_D3", "Ds", "ClubX3", 81.0, tier="SemiTop"),
        ]
        for p in in_candidates:
            p["in_cost"] = 4.0

        result = plan_market_campaign(
            roster_players=roster,
            in_candidates=in_candidates,
            credits_residual=0.0,
            max_changes=2,
            max_k=2,
            min_delta=1.0,
            min_delta_multi=1.0,
            beam_width=20,
            out_candidate_limit=10,
            in_candidate_limit=10,
            top_plans=20,
        )
        self.assertTrue(result["plans"])
        self.assertTrue(any(int(p.get("k") or 0) == 2 for p in result["plans"]))

    def test_transfer_packages_support_5x5(self):
        roster = _valid_roster()
        # Make five players weak: 2 Dif, 2 Cen, 1 Att.
        roster[3]["MarketScoreFinal"] = 20.0
        roster[4]["MarketScoreFinal"] = 21.0
        roster[10]["MarketScoreFinal"] = 19.0
        roster[11]["MarketScoreFinal"] = 18.0
        roster[18]["MarketScoreFinal"] = 17.0

        in_candidates = [
            _player("IN_D1", "Dc", "ClubY1", 88.0, tier="Top"),
            _player("IN_D2", "Dd", "ClubY2", 86.0, tier="Top"),
            _player("IN_C1", "C", "ClubY3", 87.0, tier="Top"),
            _player("IN_C2", "M", "ClubY4", 84.0, tier="SemiTop"),
            _player("IN_A1", "A", "ClubY5", 89.0, tier="Top"),
        ]
        for p in in_candidates:
            p["in_cost"] = 3.0

        result = plan_market_campaign(
            roster_players=roster,
            in_candidates=in_candidates,
            credits_residual=0.0,
            max_changes=5,
            max_k=5,
            min_delta=1.0,
            min_delta_multi=1.0,
            beam_width=30,
            out_candidate_limit=20,
            in_candidate_limit=20,
            top_plans=50,
        )
        self.assertTrue(result["plans"])
        self.assertTrue(any(int(p.get("k") or 0) == 5 for p in result["plans"]))


if __name__ == "__main__":
    unittest.main()
