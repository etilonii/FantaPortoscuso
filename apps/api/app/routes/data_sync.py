from fastapi import APIRouter

from . import data_core as core


router = APIRouter()

router.add_api_route("/internal/scheduler/run", core.internal_scheduler_run, methods=["POST"])
router.add_api_route("/admin/leghe/sync", core.admin_leghe_sync, methods=["POST"])
router.add_api_route("/admin/availability/sync", core.admin_sync_player_availability, methods=["POST"])
router.add_api_route("/admin/leghe/sync-complete", core.admin_leghe_sync_complete, methods=["POST"])
router.add_api_route("/admin/jobs/observability", core.admin_jobs_observability, methods=["GET"])
