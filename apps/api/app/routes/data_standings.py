from fastapi import APIRouter

from . import data_core as core


router = APIRouter()

router.add_api_route("/standings", core.standings, methods=["GET"])
router.add_api_route("/insights/premium", core.premium_insights, methods=["GET"])
