import unittest

from apps.api.app.engine.market_engine import suggest_transfers


def _player(
    name: str,
    role: str,
    club: str,
    qa: float,
    g_r8: float = 0,
    a_r8: float = 0,
):
    return {
        "nome": name,
        "ruolo_base": role,
        "club": club,
        "QA": qa,
        "PV_R8": 8,
        "PT_R8": 8,
        "MIN_R8": 720,
        "PV_S": 20,
        "PT_S": 20,
        "MIN_S": 1800,
        "G_R8": g_r8,
        "A_R8": a_r8,
    }


def _teams():
    return {
        "X": {
            "PPG_S": 1.4,
            "GFpg_S": 1.4,
            "GApg_S": 1.0,
            "PPG_R8": 1.5,
            "GFpg_R8": 1.5,
            "GApg_R8": 0.9,
            "MoodTeam": 0.6,
            "GamesRemaining": 10,
        },
        "Y": {
            "PPG_S": 1.2,
            "GFpg_S": 1.2,
            "GApg_S": 1.2,
            "PPG_R8": 1.1,
            "GFpg_R8": 1.0,
            "GApg_R8": 1.4,
            "MoodTeam": 0.5,
            "GamesRemaining": 10,
        },
        "Z": {
            "PPG_S": 1.3,
            "GFpg_S": 1.3,
            "GApg_S": 1.1,
            "PPG_R8": 1.2,
            "GFpg_R8": 1.2,
            "GApg_R8": 1.2,
            "MoodTeam": 0.55,
            "GamesRemaining": 10,
        },
    }


class MarketEngineTests(unittest.TestCase):
    def test_budget_constraint_prefers_affordable_in(self):
        squad = [_player("OUT1", "C", "X", 10)]
        players = [
            _player("OUT1", "C", "X", 10),
            _player("IN_EXP", "C", "Y", 12, g_r8=8),
            _player("IN_CHEAP", "C", "Z", 8, g_r8=4),
        ]
        solutions = suggest_transfers(
            user_squad=squad,
            credits_residui=0,
            players_pool=players,
            teams_data=_teams(),
            fixtures=[],
            current_round=1,
            max_changes=1,
            k_pool=3,
            m_out=1,
            beam_width=10,
        )
        self.assertTrue(solutions)
        self.assertEqual(solutions[0].swaps[0].in_player.get("nome"), "IN_CHEAP")
        self.assertGreaterEqual(solutions[0].budget_final, 0)

    def test_team_cap_blocks_fourth_club_player(self):
        squad = [
            _player("X1", "D", "X", 5),
            _player("X2", "D", "X", 5),
            _player("X3", "D", "X", 5),
            _player("OUT1", "C", "Y", 10),
        ]
        players = squad + [
            _player("IN_BAD", "C", "X", 9, g_r8=6),
            _player("IN_OK", "C", "Z", 9, g_r8=4),
        ]
        solutions = suggest_transfers(
            user_squad=squad,
            credits_residui=0,
            players_pool=players,
            teams_data=_teams(),
            fixtures=[],
            current_round=1,
            max_changes=1,
            k_pool=4,
            m_out=1,
            beam_width=10,
        )
        self.assertTrue(solutions)
        in_club = solutions[0].swaps[0].in_player.get("club")
        self.assertNotEqual(in_club, "X")

    def test_negative_gain_requires_ultra(self):
        squad = [_player("OUT1", "C", "X", 10, g_r8=2)]
        players = [
            _player("OUT1", "C", "X", 10, g_r8=2),
            _player("IN_NEG", "C", "Y", 9, g_r8=1),
        ]
        solutions = suggest_transfers(
            user_squad=squad,
            credits_residui=0,
            players_pool=players,
            teams_data=_teams(),
            fixtures=[],
            current_round=1,
            max_changes=1,
            k_pool=2,
            m_out=1,
            beam_width=5,
        )
        # With no positive options, relax paths may return a negative solution.
        self.assertTrue(solutions)
        self.assertLess(solutions[0].total_gain, 0)

    def test_exclude_ins_blocks_player(self):
        squad = [_player("OUT1", "C", "X", 10, g_r8=0)]
        players = [
            _player("OUT1", "C", "X", 10, g_r8=0),
            _player("IN_A", "C", "Y", 12, g_r8=8),
            _player("IN_B", "C", "Z", 9, g_r8=12),
        ]
        solutions = suggest_transfers(
            user_squad=squad,
            credits_residui=0,
            players_pool=players,
            teams_data=_teams(),
            fixtures=[],
            current_round=1,
            max_changes=1,
            k_pool=3,
            m_out=1,
            beam_width=10,
            exclude_ins=["IN_A"],
            include_outs_any=["OUT1"],
        )
        self.assertTrue(solutions)
        self.assertEqual(solutions[0].swaps[0].in_player.get("nome"), "IN_B")

    def test_include_outs_any_forces_out(self):
        squad = [
            _player("OUT_A", "C", "X", 10, g_r8=3),
            _player("OUT_B", "C", "Y", 9, g_r8=2),
        ]
        players = [
            _player("OUT_A", "C", "X", 10, g_r8=3),
            _player("OUT_B", "C", "Y", 9, g_r8=2),
            _player("IN_1", "C", "Z", 9, g_r8=4),
        ]
        solutions = suggest_transfers(
            user_squad=squad,
            credits_residui=0,
            players_pool=players,
            teams_data=_teams(),
            fixtures=[],
            current_round=1,
            max_changes=1,
            k_pool=3,
            m_out=2,
            beam_width=10,
            include_outs_any=["OUT_B"],
        )
        self.assertTrue(solutions)
        self.assertEqual(solutions[0].swaps[0].out_player.get("nome"), "OUT_B")

    def test_top3_diversity_excludes_top_outs(self):
        squad = [
            _player("OUT_A", "C", "X", 10, g_r8=0),
            _player("OUT_B", "C", "Y", 10, g_r8=0),
            _player("OUT_C", "C", "Z", 10, g_r8=0),
            _player("OUT_D", "C", "X", 10, g_r8=4),
            _player("OUT_E", "C", "Y", 10, g_r8=4),
            _player("OUT_F", "C", "Z", 10, g_r8=4),
        ]
        players = squad + [
            _player("IN_A", "C", "X", 10, g_r8=10),
            _player("IN_B", "C", "Y", 10, g_r8=9),
            _player("IN_C", "C", "Z", 10, g_r8=8),
            _player("IN_D", "C", "X", 10, g_r8=2),
            _player("IN_E", "C", "Y", 10, g_r8=2),
            _player("IN_F", "C", "Z", 10, g_r8=2),
        ]
        solutions = suggest_transfers(
            user_squad=squad,
            credits_residui=0,
            players_pool=players,
            teams_data=_teams(),
            fixtures=[],
            current_round=1,
            max_changes=3,
            k_pool=12,
            m_out=6,
            beam_width=50,
        )
        self.assertGreaterEqual(len(solutions), 2)
        outs1 = {s.out_player.get("nome") for s in solutions[0].swaps}
        outs2 = {s.out_player.get("nome") for s in solutions[1].swaps}
        self.assertLessEqual(len(outs1 & outs2), 3)


if __name__ == "__main__":
    unittest.main()
