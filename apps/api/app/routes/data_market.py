from fastapi import APIRouter

from . import data_core as core


router = APIRouter()

router.add_api_route("/market", core.market, methods=["GET"])
router.add_api_route("/admin/market/refresh", core.refresh_market, methods=["POST"])
router.add_api_route("/market/suggest", core.market_suggest, methods=["POST"])
router.add_api_route("/market/payload", core.market_payload, methods=["GET"])
