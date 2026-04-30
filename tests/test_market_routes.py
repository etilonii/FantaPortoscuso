import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException

from apps.api.app.engine.market_engine import Solution, Swap
from apps.api.app.routes import data_core


class MarketRoutesTests(unittest.TestCase):
    def test_market_payload_returns_scoped_team_payload(self):
        access_record = SimpleNamespace(is_admin=False)
        payload = {"user_squad": [{"Giocatore": "Rossi"}], "credits_residui": 12}

        with patch.object(data_core, "_require_login_key", return_value=access_record), patch.object(
            data_core,
            "_team_scope_for_access_key",
            return_value=("Team A", "teama"),
        ), patch.object(data_core, "_build_market_suggest_payload", return_value=payload) as build_payload:
            result = data_core.market_payload(x_access_key="abc", db=object())

        self.assertEqual(result["team"], "Team A")
        self.assertEqual(result["payload"], payload)
        build_payload.assert_called_once()

    def test_market_payload_rejects_unscoped_team_key(self):
        access_record = SimpleNamespace(is_admin=False)

        with patch.object(data_core, "_require_login_key", return_value=access_record), patch.object(
            data_core,
            "_team_scope_for_access_key",
            return_value=("", ""),
        ):
            with self.assertRaises(HTTPException) as ctx:
                data_core.market_payload(x_access_key="abc", db=object())

        self.assertEqual(ctx.exception.status_code, 403)

    def test_formazioni_non_admin_can_view_all_teams(self):
        access_record = SimpleNamespace(is_admin=False)
        projected_rows = [
            {"team": "Team A", "round": None, "standing_pos": 1, "pos": 1},
            {"team": "Team B", "round": None, "standing_pos": 2, "pos": 2},
        ]

        with patch.object(data_core, "_require_login_key", return_value=access_record), patch.object(
            data_core,
            "_team_scope_for_access_key",
            return_value=("Team A", "teama"),
        ), patch.object(data_core, "_load_regulation", return_value={}), patch.object(
            data_core,
            "_reg_ordering",
            return_value=("classifica", ["classifica", "live_total"]),
        ), patch.object(data_core, "_build_standings_index", return_value={}), patch.object(
            data_core,
            "_load_status_matchday",
            return_value=35,
        ), patch.object(data_core, "_leghe_sync_reference_round_now", return_value=None), patch.object(
            data_core,
            "_latest_round_with_live_votes",
            return_value=None,
        ), patch.object(data_core, "_load_club_name_index", return_value={}), patch.object(
            data_core,
            "_load_seriea_fixtures_for_insights",
            return_value=[],
        ), patch.object(data_core, "_is_formazioni_real_unlocked_for_round", return_value=(False, None, "")), patch.object(
            data_core,
            "_load_fixture_rows_for_live",
            return_value=[{"round": 35}],
        ), patch.object(data_core, "_resolve_formazioni_optimizer_round", return_value=35), patch.object(
            data_core,
            "_load_classifica_positions",
            return_value={},
        ), patch.object(data_core, "_load_live_standings_positions", return_value={}), patch.object(
            data_core,
            "_load_projected_formazioni_rows",
            return_value=projected_rows,
        ) as load_projected, patch.object(data_core, "_load_live_round_context", return_value={}), patch.object(
            data_core,
            "_attach_live_scores_to_formations",
            return_value=None,
        ):
            result = data_core.formazioni(
                team=None,
                round=None,
                order_by=None,
                x_access_key="abc",
                db=object(),
                limit=200,
            )

        self.assertEqual([item["team"] for item in result["items"]], ["Team A", "Team B"])
        self.assertFalse(result["team_scope_enforced"])
        self.assertFalse(result["team_scope_missing"])
        load_projected.assert_called_once()
        self.assertEqual(load_projected.call_args.args[0], "")

    def test_market_suggest_serializes_engine_response(self):
        captured = {}

        def fake_suggest_transfers(**kwargs):
            captured.update(kwargs)
            return [
                Solution(
                    swaps=[
                        Swap(
                            out_player={"Giocatore": "Old Mid", "Ruolo": "C", "Squadra": "AAA"},
                            in_player={"nome": "New Mid", "ruolo_base": "C", "club": "BBB"},
                            gain=4.25,
                            qa_out=12,
                            qa_in=9,
                        )
                    ],
                    budget_initial=7,
                    budget_final=10,
                    total_gain=4.25,
                    recommended_outs=["Old Mid"],
                    warnings=["Presente cambio negativo"],
                )
            ]

        payload = {
            "user_squad": [{"Giocatore": "Old Mid", "Ruolo": "C", "Squadra": "AAA"}],
            "credits_residui": "7",
            "players_pool": [{"nome": "New Mid", "ruolo_base": "C", "club": "BBB", "QA": 9}],
            "teams_data": {"AAA": {"PPG_S": 1.0}, "BBB": {"PPG_S": 1.1}},
            "fixtures": [{"round": 28, "team": "AAA", "opponent": "BBB", "home_away": "H"}],
            "currentRound": "28",
            "params": {
                "max_changes": "2",
                "k_pool": "30",
                "m_out": "3",
                "beam_width": "120",
                "require_roles": ["c", "A"],
                "required_outs": ["Old Mid", "Old Mid"],
                "exclude_ins": ["Blocked", ""],
                "fixed_swaps": [["Old Mid", "New Mid"]],
                "include_outs_any": ["Old Mid"],
            },
        }

        with patch.object(data_core, "suggest_transfers", side_effect=fake_suggest_transfers):
            result = data_core.market_suggest(payload)

        self.assertEqual(captured["current_round"], 28)
        self.assertEqual(captured["max_changes"], 2)
        self.assertEqual(captured["k_pool"], 30)
        self.assertEqual(captured["m_out"], 3)
        self.assertEqual(captured["beam_width"], 120)
        self.assertEqual(captured["required_outs"], ["Old Mid"])
        self.assertEqual(captured["exclude_ins"], ["Blocked"])
        self.assertEqual(captured["fixed_swaps"], [("Old Mid", "New Mid")])
        self.assertEqual(captured["include_outs_any"], ["Old Mid"])
        self.assertEqual(captured["require_roles"], {"C", "A"})

        self.assertEqual(len(result["solutions"]), 1)
        solution = result["solutions"][0]
        self.assertEqual(solution["budget_final"], 10.0)
        self.assertEqual(solution["total_gain"], 4.25)
        self.assertEqual(solution["recommended_outs"], ["Old Mid"])
        self.assertEqual(solution["warnings"], ["Presente cambio negativo"])
        self.assertEqual(solution["swaps"][0]["out"], "Old Mid")
        self.assertEqual(solution["swaps"][0]["in"], "New Mid")
        self.assertEqual(solution["swaps"][0]["qa_out"], 12.0)
        self.assertEqual(solution["swaps"][0]["qa_in"], 9.0)
        self.assertEqual(solution["swaps"][0]["delta"], 3.0)


if __name__ == "__main__":
    unittest.main()
