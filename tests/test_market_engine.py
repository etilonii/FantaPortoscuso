import unittest

from apps.api.app.engine.market_engine import suggest_transfers


class MarketEngineTests(unittest.TestCase):
    def test_constraints_budget_and_negative(self):
        players = [
            {"nome": "A1", "ruolo_base": "C", "club": "X", "QA": 10, "PV_R8": 4, "PT_R8": 4, "MIN_R8": 360},
            {"nome": "A2", "ruolo_base": "C", "club": "X", "QA": 12, "PV_R8": 4, "PT_R8": 4, "MIN_R8": 360},
            {"nome": "B1", "ruolo_base": "C", "club": "Y", "QA": 8, "PV_R8": 4, "PT_R8": 1, "MIN_R8": 90},
        ]
        teams = {
            "X": {"PPG_S": 1.4, "GFpg_S": 1.2, "GApg_S": 1.0, "PPG_R8": 1.5, "GFpg_R8": 1.1, "GApg_R8": 0.9},
            "Y": {"PPG_S": 1.2, "GFpg_S": 1.0, "GApg_S": 1.2, "PPG_R8": 1.1, "GFpg_R8": 0.9, "GApg_R8": 1.4},
        }
        fixtures = []
        squad = [{"nome": "B1", "ruolo_base": "C", "club": "Y", "QA": 8, "PV_R8": 4, "PT_R8": 1, "MIN_R8": 90}]
        solutions = suggest_transfers(
            user_squad=squad,
            credits_residui=0,
            players_pool=players,
            teams_data=teams,
            fixtures=fixtures,
            current_round=1,
            max_changes=1,
            k_pool=3,
            m_out=1,
            beam_width=5,
        )
        self.assertTrue(all(sol.budget_final >= 0 for sol in solutions))


if __name__ == "__main__":
    unittest.main()
