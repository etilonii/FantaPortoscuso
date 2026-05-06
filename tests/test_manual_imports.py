from __future__ import annotations

import asyncio
import shutil
import uuid
from io import BytesIO
from pathlib import Path

import pytest
from fastapi import HTTPException

from apps.api.app.services import manual_imports as mi


@pytest.fixture
def local_tmp_dir():
    root = Path.cwd() / ".test-tmp" / f"manual-import-{uuid.uuid4().hex}"
    root.mkdir(parents=True, exist_ok=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _configure_manual_import_paths(monkeypatch, local_tmp_dir: Path) -> dict[str, Path]:
    data_dir = local_tmp_dir / "data"
    active_rose = data_dir / "rose_fantaportoscuso.csv"
    active_quot = data_dir / "quotazioni.csv"
    active_formazioni = data_dir / "reports" / "formazioni_giornata.csv"
    active_rose.parent.mkdir(parents=True, exist_ok=True)
    active_rose.write_text(
        "Team,Giocatore,Ruolo,Squadra,PrezzoAcquisto,PrezzoAttuale\n"
        "Old Team,Old Player,A,OLD,1,1\n",
        encoding="utf-8",
    )
    active_quot.write_text(
        "Id,Ruolo,RuoloMantra,Giocatore,Squadra,PrezzoAttuale,PrezzoIniziale,FVM\n"
        "1,A,A,Old Player,OLD,1,1,1\n",
        encoding="utf-8",
    )
    active_formazioni.parent.mkdir(parents=True, exist_ok=True)
    active_formazioni.write_text(
        "giornata,team,modulo,portiere,difensori,centrocampisti,attaccanti,panchina\n"
        "1,Old Team,3-4-3,Old Keeper,D1;D2;D3,C1;C2;C3;C4,A1;A2;A3,Res1;Res2\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(mi, "DATA_DIR", data_dir)
    monkeypatch.setattr(mi, "MANUAL_IMPORT_STATUS_PATH", data_dir / "manual_import_status.json")
    monkeypatch.setattr(mi, "MANUAL_INCOMING_DIR", data_dir / "incoming" / "manual")
    monkeypatch.setattr(mi, "MANUAL_BACKUP_DIR", data_dir / "backups" / "manual_imports")
    monkeypatch.setattr(
        mi,
        "ACTIVE_PATHS",
        {"rose": active_rose, "quotazioni": active_quot, "formazioni": active_formazioni},
    )
    return {"rose": active_rose, "quotazioni": active_quot, "formazioni": active_formazioni, "data": data_dir}


def _bytes(text: str) -> BytesIO:
    return BytesIO(text.encode("utf-8"))


def test_manual_import_rose_valid_updates_active_file_and_status(monkeypatch, local_tmp_dir):
    paths = _configure_manual_import_paths(monkeypatch, local_tmp_dir)

    result = mi.save_and_activate_manual_import(
        "rose",
        original_filename="rose.csv",
        fileobj=_bytes(
            "Team,Giocatore,Ruolo,Squadra,PrezzoAcquisto,PrezzoAttuale\n"
            "Pi-Ciaccio,David,A,JUV,59.42,70.11\n"
        ),
    )

    active_text = paths["rose"].read_text(encoding="utf-8")
    status = mi.load_manual_import_status()["rose"]
    assert result["status"] == "ok"
    assert result["imported_rows"] == 1
    assert "Pi-Ciaccio" in active_text
    assert "Old Player" not in active_text
    assert status["status"] == "ok"
    assert status["imported_rows"] == 1
    assert Path(status["stored_path"]).exists()
    assert Path(status["backup_path"]).exists()


def test_manual_import_rose_missing_columns_does_not_overwrite_active_file(monkeypatch, local_tmp_dir):
    paths = _configure_manual_import_paths(monkeypatch, local_tmp_dir)
    before = paths["rose"].read_text(encoding="utf-8")

    result = mi.save_and_activate_manual_import(
        "rose",
        original_filename="rose.csv",
        fileobj=_bytes("Team,Giocatore\nPi-Ciaccio,David\n"),
    )

    assert result["status"] == "error"
    assert "Colonne mancanti rose" in result["errors"][0]
    assert paths["rose"].read_text(encoding="utf-8") == before
    assert mi.load_manual_import_status()["rose"]["status"] == "error"


def test_manual_import_quotazioni_valid_updates_active_file_and_status(monkeypatch, local_tmp_dir):
    paths = _configure_manual_import_paths(monkeypatch, local_tmp_dir)

    result = mi.save_and_activate_manual_import(
        "quotazioni",
        original_filename="quotazioni.csv",
        fileobj=_bytes(
            "Id,Ruolo,RuoloMantra,Giocatore,Squadra,PrezzoAttuale,PrezzoIniziale,FVM\n"
            "10,A,A,David,JUV,70.11,59.42,85.5\n"
        ),
    )

    active_text = paths["quotazioni"].read_text(encoding="utf-8")
    status = mi.load_manual_import_status()["quotazioni"]
    assert result["status"] == "ok"
    assert result["imported_rows"] == 1
    assert "David" in active_text
    assert "Old Player" not in active_text
    assert status["status"] == "ok"
    assert status["imported_rows"] == 1


def test_manual_import_quotazioni_invalid_price_does_not_overwrite_active_file(monkeypatch, local_tmp_dir):
    paths = _configure_manual_import_paths(monkeypatch, local_tmp_dir)
    before = paths["quotazioni"].read_text(encoding="utf-8")

    result = mi.save_and_activate_manual_import(
        "quotazioni",
        original_filename="quotazioni.csv",
        fileobj=_bytes("Giocatore,Ruolo,Squadra,PrezzoAttuale\nDavid,A,JUV,non-numero\n"),
    )

    assert result["status"] == "error"
    assert any("PrezzoAttuale" in error for error in result["errors"])
    assert paths["quotazioni"].read_text(encoding="utf-8") == before
    assert mi.load_manual_import_status()["quotazioni"]["status"] == "error"


def test_manual_import_formazioni_valid_updates_active_file_and_status(monkeypatch, local_tmp_dir):
    paths = _configure_manual_import_paths(monkeypatch, local_tmp_dir)

    result = mi.save_and_activate_manual_import(
        "formazioni",
        original_filename="formazioni.csv",
        fileobj=_bytes(
            "giornata,team,modulo,portiere,difensori,centrocampisti,attaccanti,panchina\n"
            "35,Pi-Ciaccio,343,David G,Def1;Def2;Def3,Cen1;Cen2;Cen3;Cen4,Att1;Att2;Att3,Res1;Res2\n"
        ),
    )

    active_text = paths["formazioni"].read_text(encoding="utf-8")
    status = mi.load_manual_import_status()["formazioni"]
    assert result["status"] == "ok"
    assert result["imported_rows"] == 1
    assert result["rounds_detected"] == [35]
    assert result["teams_detected"] == ["Pi-Ciaccio"]
    assert "Pi-Ciaccio" in active_text
    assert "3-4-3" in active_text
    assert status["status"] == "ok"
    assert status["rounds_detected"] == [35]


def test_manual_import_formazioni_missing_team_does_not_overwrite_active_file(monkeypatch, local_tmp_dir):
    paths = _configure_manual_import_paths(monkeypatch, local_tmp_dir)
    before = paths["formazioni"].read_text(encoding="utf-8")

    result = mi.save_and_activate_manual_import(
        "formazioni",
        original_filename="formazioni.csv",
        fileobj=_bytes(
            "giornata,team,modulo,portiere,difensori,centrocampisti,attaccanti,panchina\n"
            "35,,343,David G,Def1;Def2;Def3,Cen1;Cen2;Cen3;Cen4,Att1;Att2;Att3,\n"
        ),
    )

    assert result["status"] == "error"
    assert any("team non vuoto" in error.lower() for error in result["errors"])
    assert paths["formazioni"].read_text(encoding="utf-8") == before
    assert mi.load_manual_import_status()["formazioni"]["status"] == "error"


def test_manual_import_formazioni_few_starters_returns_warning(monkeypatch, local_tmp_dir):
    _configure_manual_import_paths(monkeypatch, local_tmp_dir)

    result = mi.save_and_activate_manual_import(
        "formazioni",
        original_filename="formazioni.csv",
        fileobj=_bytes(
            "giornata,team,modulo,portiere,difensori,centrocampisti,attaccanti,panchina\n"
            "35,Pi-Ciaccio,343,David G,Def1;Def2,Cen1;Cen2;Cen3,Att1;Att2,\n"
        ),
    )

    assert result["status"] == "ok"
    assert any("titolari" in warning.lower() for warning in result["warnings"])


def test_manual_import_endpoint_blocked_when_manual_imports_disabled(monkeypatch):
    from apps.api.app.routes import data

    monkeypatch.setattr(data, "ENABLE_MANUAL_IMPORTS", False)
    monkeypatch.setattr(data, "_require_admin_key", lambda *_args, **_kwargs: None)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            data.admin_manual_import_rose(
                request=object(),
                db=object(),
                x_admin_key="admin",
                authorization=None,
            )
        )

    assert exc.value.status_code == 403
    assert exc.value.detail["reason"] == "manual_imports_disabled"


def test_manual_import_formazioni_endpoint_blocked_when_manual_imports_disabled(monkeypatch):
    from apps.api.app.routes import data

    monkeypatch.setattr(data, "ENABLE_MANUAL_IMPORTS", False)
    monkeypatch.setattr(data, "_require_admin_key", lambda *_args, **_kwargs: None)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            data.admin_manual_import_formazioni(
                request=object(),
                db=object(),
                x_admin_key="admin",
                authorization=None,
            )
        )

    assert exc.value.status_code == 403
    assert exc.value.detail["reason"] == "manual_imports_disabled"
