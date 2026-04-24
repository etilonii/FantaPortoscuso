from fastapi import APIRouter

from . import data_core as core


router = APIRouter()

router.add_api_route("/live/payload", core.live_payload, methods=["GET"])
router.add_api_route("/live/match-six", core.set_live_match_six, methods=["POST"])
router.add_api_route("/live/player", core.upsert_live_player_vote, methods=["POST"])
router.add_api_route("/live/import-voti", core.import_live_votes, methods=["POST"])
