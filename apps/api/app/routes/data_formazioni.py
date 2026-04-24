from fastapi import APIRouter

from . import data_core as core


router = APIRouter()

router.add_api_route("/formazioni", core.formazioni, methods=["GET"])
router.add_api_route("/formazioni/optimizer", core.formazione_optimizer, methods=["GET"])
