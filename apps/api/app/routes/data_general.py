from fastapi import APIRouter

from . import data_core as core


router = APIRouter()

router.add_api_route("/summary", core.summary, methods=["GET"])
router.add_api_route("/players", core.players, methods=["GET"])
router.add_api_route("/quotazioni", core.quotazioni, methods=["GET"])
router.add_api_route("/listone", core.listone, methods=["GET"])
router.add_api_route("/teams", core.teams, methods=["GET"])
router.add_api_route("/top-acquisti", core.top_acquisti, methods=["GET"])
router.add_api_route("/team/{team_name}", core.team_roster, methods=["GET"])
router.add_api_route("/stats/plusvalenze", core.stats_plusvalenze, methods=["GET"])
router.add_api_route("/stats/players", core.stats_players, methods=["GET"])
router.add_api_route("/stats/player", core.stats_player, methods=["GET"])
router.add_api_route("/stats/{stat_name}", core.stats_by_stat, methods=["GET"])
