from __future__ import annotations

from fastapi import APIRouter

from . import data_core as _core
from .data_formazioni import router as _formazioni_router
from .data_general import router as _general_router
from .data_live import router as _live_router
from .data_market import router as _market_router
from .data_standings import router as _standings_router
from .data_sync import router as _sync_router


router = APIRouter(prefix="/data", tags=["data"])
for _child_router in (
    _general_router,
    _standings_router,
    _live_router,
    _sync_router,
    _formazioni_router,
    _market_router,
):
    router.include_router(_child_router)


# Mutable/shared state exposed for tests and runtime callers.
_FORMAZIONI_REMOTE_REFRESH_CACHE = _core._FORMAZIONI_REMOTE_REFRESH_CACHE
_AUTO_VOTI_IMPORT_ATTEMPTED_ROUNDS = _core._AUTO_VOTI_IMPORT_ATTEMPTED_ROUNDS
_SYNC_COMPLETE_BACKGROUND_LOCK = _core._SYNC_COMPLETE_BACKGROUND_LOCK
_SYNC_COMPLETE_BACKGROUND_RUNNING = _core._SYNC_COMPLETE_BACKGROUND_RUNNING


# Config and helper symbols that tests monkeypatch on this module.
subprocess = _core.subprocess
LEGHE_ALIAS = _core.LEGHE_ALIAS
LEGHE_USERNAME = _core.LEGHE_USERNAME
LEGHE_PASSWORD = _core.LEGHE_PASSWORD
LEGHE_COMPETITION_ID = _core.LEGHE_COMPETITION_ID
LEGHE_COMPETITION_NAME = _core.LEGHE_COMPETITION_NAME
LEGHE_FORMATIONS_MATCHDAY = _core.LEGHE_FORMATIONS_MATCHDAY
LEGHE_SYNC_TZ = _core.LEGHE_SYNC_TZ
LEGHE_SYNC_WINDOWS = _core.LEGHE_SYNC_WINDOWS
LEGHE_SYNC_SLOT_HOURS = _core.LEGHE_SYNC_SLOT_HOURS
LEGHE_MATCHDAY_SYNC_START_HOUR_MON_SAT = _core.LEGHE_MATCHDAY_SYNC_START_HOUR_MON_SAT
LEGHE_MATCHDAY_SYNC_START_HOUR_SUN = _core.LEGHE_MATCHDAY_SYNC_START_HOUR_SUN
REAL_FORMATIONS_TMP_DIR = _core.REAL_FORMATIONS_TMP_DIR

_context_html_candidates = _core._context_html_candidates
_download_formazioni_pagina_payload = _core._download_formazioni_pagina_payload
_load_standings_rows = _core._load_standings_rows
_build_standings_index = _core._build_standings_index
_load_status_matchday = _core._load_status_matchday
_infer_matchday_from_fixtures = _core._infer_matchday_from_fixtures
_infer_matchday_from_stats = _core._infer_matchday_from_stats
_load_real_formazioni_rows = _core._load_real_formazioni_rows
_load_live_round_context = _core._load_live_round_context
_attach_live_scores_to_formations = _core._attach_live_scores_to_formations
_latest_round_with_live_votes = _core._latest_round_with_live_votes
_is_round_completed_from_fixtures = _core._is_round_completed_from_fixtures
_run_live_import_for_round_safe = _core._run_live_import_for_round_safe
_import_live_votes_internal = _core._import_live_votes_internal
_claim_scheduled_job_run = _core._claim_scheduled_job_run
_leghe_sync_reference_round_now = _core._leghe_sync_reference_round_now
_normalize_season_slug = _core._normalize_season_slug
run_leghe_sync_and_pipeline = _core.run_leghe_sync_and_pipeline
_sync_complete_background_worker = _core._sync_complete_background_worker


_SYNCABLE_NAMES = (
    "subprocess",
    "LEGHE_ALIAS",
    "LEGHE_USERNAME",
    "LEGHE_PASSWORD",
    "LEGHE_COMPETITION_ID",
    "LEGHE_COMPETITION_NAME",
    "LEGHE_FORMATIONS_MATCHDAY",
    "LEGHE_SYNC_TZ",
    "LEGHE_SYNC_WINDOWS",
    "LEGHE_SYNC_SLOT_HOURS",
    "LEGHE_MATCHDAY_SYNC_START_HOUR_MON_SAT",
    "LEGHE_MATCHDAY_SYNC_START_HOUR_SUN",
    "REAL_FORMATIONS_TMP_DIR",
    "_FORMAZIONI_REMOTE_REFRESH_CACHE",
    "_AUTO_VOTI_IMPORT_ATTEMPTED_ROUNDS",
    "_SYNC_COMPLETE_BACKGROUND_LOCK",
    "_SYNC_COMPLETE_BACKGROUND_RUNNING",
    "_context_html_candidates",
    "_download_formazioni_pagina_payload",
    "_load_standings_rows",
    "_build_standings_index",
    "_load_status_matchday",
    "_infer_matchday_from_fixtures",
    "_infer_matchday_from_stats",
    "_load_real_formazioni_rows",
    "_load_live_round_context",
    "_attach_live_scores_to_formations",
    "_latest_round_with_live_votes",
    "_is_round_completed_from_fixtures",
    "_run_live_import_for_round_safe",
    "_import_live_votes_internal",
    "_claim_scheduled_job_run",
    "_leghe_sync_reference_round_now",
    "_normalize_season_slug",
    "run_leghe_sync_and_pipeline",
    "_sync_complete_background_worker",
)
_PULLBACK_NAMES = ("_SYNC_COMPLETE_BACKGROUND_RUNNING",)


def _sync_core_state() -> None:
    module_globals = globals()
    for name in _SYNCABLE_NAMES:
        if name in module_globals:
            setattr(_core, name, module_globals[name])


def _pull_core_state() -> None:
    module_globals = globals()
    for name in _PULLBACK_NAMES:
        module_globals[name] = getattr(_core, name)


def _wrap_core(name: str):
    target = getattr(_core, name)

    def _wrapped(*args, **kwargs):
        _sync_core_state()
        try:
            return target(*args, **kwargs)
        finally:
            _pull_core_state()

    _wrapped.__name__ = name
    _wrapped.__doc__ = getattr(target, "__doc__", None)
    return _wrapped


# Direct helper exports used by tests and other callers.
_default_regulation = _core._default_regulation
_reg_bonus_map = _core._reg_bonus_map
_reg_appkey_bonus_indexes = _core._reg_appkey_bonus_indexes
_appkey_bonus_event_counts = _core._appkey_bonus_event_counts
_overlay_decisive_badges_from_appkey = _core._overlay_decisive_badges_from_appkey
_infer_decisive_events_from_fantavote = _core._infer_decisive_events_from_fantavote
_extract_formazioni_tmp_entries_from_html = _core._extract_formazioni_tmp_entries_from_html
_build_seriea_live_snapshot = _core._build_seriea_live_snapshot


# Wrapped helpers whose internals depend on globals monkeypatched in tests.
_refresh_formazioni_appkey_from_context_html = _wrap_core(
    "_refresh_formazioni_appkey_from_context_html"
)
_leghe_sync_round_for_local_dt = _wrap_core("_leghe_sync_round_for_local_dt")
_leghe_sync_reference_round_with_lookahead = _wrap_core(
    "_leghe_sync_reference_round_with_lookahead"
)
_leghe_sync_slot_start_local = _wrap_core("_leghe_sync_slot_start_local")
leghe_sync_seconds_until_next_slot = _wrap_core("leghe_sync_seconds_until_next_slot")
_backfill_standings_played_if_missing = _wrap_core("_backfill_standings_played_if_missing")
_build_live_standings_rows = _wrap_core("_build_live_standings_rows")
_enqueue_sync_complete_background = _wrap_core("_enqueue_sync_complete_background")
run_auto_leghe_sync = _wrap_core("run_auto_leghe_sync")
run_auto_seriea_live_context_sync = _wrap_core("run_auto_seriea_live_context_sync")


def __getattr__(name: str):
    return getattr(_core, name)
