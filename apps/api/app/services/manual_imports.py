from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
import re
from typing import BinaryIO, Literal

import pandas as pd


ManualImportSource = Literal["rose", "quotazioni", "formazioni"]

DATA_DIR = Path(__file__).resolve().parents[4] / "data"
MANUAL_IMPORT_STATUS_PATH = DATA_DIR / "manual_import_status.json"
MANUAL_INCOMING_DIR = DATA_DIR / "incoming" / "manual"
MANUAL_BACKUP_DIR = DATA_DIR / "backups" / "manual_imports"
ACTIVE_PATHS: dict[ManualImportSource, Path] = {
    "rose": DATA_DIR / "rose_fantaportoscuso.csv",
    "quotazioni": DATA_DIR / "quotazioni.csv",
    "formazioni": DATA_DIR / "reports" / "formazioni_giornata.csv",
}

SUPPORTED_EXTENSIONS = {".csv", ".xlsx"}
ROLES = {"P", "D", "C", "A"}
FORMATION_COLUMNS = [
    "giornata",
    "team",
    "modulo",
    "portiere",
    "difensori",
    "centrocampisti",
    "attaccanti",
    "panchina",
]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _timestamp_for_filename(now: datetime | None = None) -> str:
    return (now or _utc_now()).astimezone(timezone.utc).strftime("%Y-%m-%d_%H%M%S")


def _status_timestamp(now: datetime | None = None) -> str:
    return (now or _utc_now()).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _norm_col(value: object) -> str:
    return "".join(ch for ch in str(value or "").strip().lower() if ch.isalnum())


def _clean_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def _read_source(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        try:
            return pd.read_csv(path, encoding="utf-8-sig", sep=None, engine="python")
        except UnicodeDecodeError:
            return pd.read_csv(path, encoding="latin-1", sep=None, engine="python")
    if suffix == ".xlsx":
        return pd.read_excel(path)
    if suffix == ".xls":
        raise ValueError("Formato .xls non supportato senza dipendenza xlrd. Usa CSV o XLSX.")
    raise ValueError("Formato file non supportato. Usa CSV o XLSX.")


def _rename_by_alias(df: pd.DataFrame, alias_map: dict[str, str]) -> pd.DataFrame:
    rename: dict[object, str] = {}
    for col in df.columns:
        target = alias_map.get(_norm_col(col))
        if target:
            rename[col] = target
    return df.rename(columns=rename)


def _to_number_series(series: pd.Series, *, column: str, errors: list[str]) -> pd.Series:
    raw = series.map(_clean_text).str.replace(",", ".", regex=False)
    parsed = pd.to_numeric(raw, errors="coerce")
    invalid = raw.ne("") & parsed.isna()
    if invalid.any():
        rows = [str(int(idx) + 2) for idx in list(series.index[invalid])[:10]]
        errors.append(f"Colonna {column}: valori non numerici alle righe {', '.join(rows)}")
    return parsed.fillna(0)


def _drop_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    clean = df.copy()
    for col in clean.columns:
        clean[col] = clean[col].map(_clean_text)
    return clean.loc[clean.apply(lambda row: any(str(value).strip() for value in row), axis=1)].copy()


def _split_players(value: object) -> list[str]:
    raw = _clean_text(value)
    if not raw:
        return []
    return [item.strip() for item in re.split(r"[;\n|]+", raw) if item and item.strip()]


def _normalize_module(value: object) -> str:
    raw = re.sub(r"[^0-9]", "", _clean_text(value))
    if len(raw) != 3:
        return _clean_text(value)
    try:
        if sum(int(ch) for ch in raw) != 10:
            return _clean_text(value)
    except ValueError:
        return _clean_text(value)
    return f"{raw[0]}-{raw[1]}-{raw[2]}"


def _validate_rose(path: Path) -> tuple[pd.DataFrame, list[str], list[str]]:
    warnings: list[str] = []
    errors: list[str] = []
    df = _drop_empty_rows(_read_source(path))
    alias_map = {
        "team": "Team",
        "fantateam": "Team",
        "squadrautente": "Team",
        "giocatore": "Giocatore",
        "calciatore": "Giocatore",
        "nome": "Giocatore",
        "ruolo": "Ruolo",
        "squadra": "Squadra",
        "club": "Squadra",
        "prezzoacquisto": "PrezzoAcquisto",
        "qacq": "PrezzoAcquisto",
        "costo": "PrezzoAcquisto",
        "prezzoattuale": "PrezzoAttuale",
        "qatt": "PrezzoAttuale",
        "qa": "PrezzoAttuale",
    }
    df = _rename_by_alias(df, alias_map)
    required = ["Team", "Giocatore", "Ruolo", "Squadra", "PrezzoAcquisto", "PrezzoAttuale"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        errors.append(f"Colonne mancanti rose: {', '.join(missing)}")
        return pd.DataFrame(columns=required), warnings, errors

    out = df[required].copy()
    for col in ["Team", "Giocatore", "Ruolo", "Squadra"]:
        out[col] = out[col].map(_clean_text)
    out["Ruolo"] = out["Ruolo"].str[:1].str.upper()
    out["PrezzoAcquisto"] = _to_number_series(out["PrezzoAcquisto"], column="PrezzoAcquisto", errors=errors).round(2)
    out["PrezzoAttuale"] = _to_number_series(out["PrezzoAttuale"], column="PrezzoAttuale", errors=errors).round(2)

    if out["Team"].eq("").any():
        errors.append("Rose: Team non vuoto richiesto.")
    if out["Giocatore"].eq("").any():
        errors.append("Rose: Giocatore non vuoto richiesto.")

    invalid_roles = sorted(set(out.loc[~out["Ruolo"].isin(ROLES), "Ruolo"].dropna().astype(str)) - {""})
    if invalid_roles:
        errors.append(f"Rose: ruoli non validi: {', '.join(invalid_roles)}")

    duplicate_mask = out.duplicated(subset=["Team", "Giocatore"], keep=False)
    if duplicate_mask.any():
        sample = out.loc[duplicate_mask, ["Team", "Giocatore"]].head(5)
        labels = [f"{row.Team}+{row.Giocatore}" for row in sample.itertuples(index=False)]
        errors.append(f"Rose: duplicati Team+Giocatore: {', '.join(labels)}")

    if out.empty:
        errors.append("Rose: nessuna riga valida trovata.")
    if errors:
        return out, warnings, errors

    out = out.sort_values(["Team", "Ruolo", "Giocatore"]).reset_index(drop=True)
    return out, warnings, errors


def _validate_quotazioni(path: Path) -> tuple[pd.DataFrame, list[str], list[str]]:
    warnings: list[str] = []
    errors: list[str] = []
    df = _drop_empty_rows(_read_source(path))
    alias_map = {
        "id": "Id",
        "ruolo": "Ruolo",
        "r": "Ruolo",
        "ruolomantra": "RuoloMantra",
        "giocatore": "Giocatore",
        "calciatore": "Giocatore",
        "nome": "Giocatore",
        "squadra": "Squadra",
        "club": "Squadra",
        "prezzoattuale": "PrezzoAttuale",
        "qta": "PrezzoAttuale",
        "qa": "PrezzoAttuale",
        "quotazione": "PrezzoAttuale",
        "prezzoiniziale": "PrezzoIniziale",
        "qti": "PrezzoIniziale",
        "fvm": "FVM",
    }
    df = _rename_by_alias(df, alias_map)
    required = ["Giocatore", "PrezzoAttuale"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        errors.append(f"Colonne mancanti quotazioni: {', '.join(missing)}")
        return pd.DataFrame(columns=["Id", "Ruolo", "RuoloMantra", "Giocatore", "Squadra", "PrezzoAttuale", "PrezzoIniziale", "FVM"]), warnings, errors

    for col in ["Id", "Ruolo", "RuoloMantra", "Squadra", "PrezzoIniziale", "FVM"]:
        if col not in df.columns:
            df[col] = ""

    out_cols = ["Id", "Ruolo", "RuoloMantra", "Giocatore", "Squadra", "PrezzoAttuale", "PrezzoIniziale", "FVM"]
    out = df[out_cols].copy()
    for col in ["Id", "Ruolo", "RuoloMantra", "Giocatore", "Squadra"]:
        out[col] = out[col].map(_clean_text)
    out["Ruolo"] = out["Ruolo"].str[:1].str.upper()
    out["PrezzoAttuale"] = _to_number_series(out["PrezzoAttuale"], column="PrezzoAttuale", errors=errors).round(2)
    out["PrezzoIniziale"] = _to_number_series(out["PrezzoIniziale"], column="PrezzoIniziale", errors=errors).round(2)
    out["FVM"] = _to_number_series(out["FVM"], column="FVM", errors=errors).round(2)

    if out["Giocatore"].eq("").any():
        errors.append("Quotazioni: Giocatore non vuoto richiesto.")

    non_empty_roles = out["Ruolo"].ne("")
    invalid_roles = sorted(set(out.loc[non_empty_roles & ~out["Ruolo"].isin(ROLES), "Ruolo"].dropna().astype(str)))
    if invalid_roles:
        errors.append(f"Quotazioni: ruoli non validi: {', '.join(invalid_roles)}")

    if out.empty:
        errors.append("Quotazioni: nessuna riga valida trovata.")
    if errors:
        return out, warnings, errors

    duplicate_mask = out["Giocatore"].str.lower().duplicated(keep="last")
    if duplicate_mask.any():
        warnings.append("Quotazioni: giocatori duplicati rimossi mantenendo l'ultima occorrenza.")
        out = out.loc[~duplicate_mask].copy()

    out = out.sort_values(["PrezzoAttuale", "Giocatore"], ascending=[False, True]).reset_index(drop=True)
    return out, warnings, errors


def _validate_formazioni(path: Path) -> tuple[pd.DataFrame, list[str], list[str]]:
    warnings: list[str] = []
    errors: list[str] = []
    df = _drop_empty_rows(_read_source(path))
    alias_map = {
        "giornata": "giornata",
        "round": "giornata",
        "matchday": "giornata",
        "turno": "giornata",
        "team": "team",
        "squadra": "team",
        "fantateam": "team",
        "fantasquadra": "team",
        "modulo": "modulo",
        "formation": "modulo",
        "schema": "modulo",
        "portiere": "portiere",
        "p": "portiere",
        "gk": "portiere",
        "difensori": "difensori",
        "difesa": "difensori",
        "d": "difensori",
        "centrocampisti": "centrocampisti",
        "centrocampo": "centrocampisti",
        "c": "centrocampisti",
        "attaccanti": "attaccanti",
        "attacco": "attaccanti",
        "a": "attaccanti",
        "panchina": "panchina",
        "riserve": "panchina",
        "bench": "panchina",
    }
    df = _rename_by_alias(df, alias_map)
    missing = [col for col in FORMATION_COLUMNS if col not in df.columns]
    if missing:
        errors.append(f"Colonne mancanti formazioni: {', '.join(missing)}")
        return pd.DataFrame(columns=FORMATION_COLUMNS), warnings, errors

    out = df[FORMATION_COLUMNS].copy()
    for col in FORMATION_COLUMNS:
        out[col] = out[col].map(_clean_text)

    parsed_rounds = pd.to_numeric(out["giornata"], errors="coerce")
    invalid_rounds = out["giornata"].ne("") & (parsed_rounds.isna() | (parsed_rounds <= 0))
    if invalid_rounds.any():
        rows = [str(int(idx) + 2) for idx in list(out.index[invalid_rounds])[:10]]
        errors.append(f"Formazioni: giornata non valida alle righe {', '.join(rows)}")
    out["giornata"] = parsed_rounds.fillna(0).astype(int).astype(str)
    out.loc[parsed_rounds.isna(), "giornata"] = ""
    out.loc[parsed_rounds <= 0, "giornata"] = ""

    if out["team"].eq("").any():
        errors.append("Formazioni: team non vuoto richiesto.")

    lineup_sizes: list[int] = []
    for idx, row in out.iterrows():
        portiere = _split_players(row.get("portiere"))
        difensori = _split_players(row.get("difensori"))
        centrocampisti = _split_players(row.get("centrocampisti"))
        attaccanti = _split_players(row.get("attaccanti"))
        if not (portiere or difensori or centrocampisti or attaccanti):
            errors.append(f"Formazioni: nessun titolare indicato alla riga {int(idx) + 2}")
            continue

        starters_count = len(portiere[:1]) + len(difensori) + len(centrocampisti) + len(attaccanti)
        lineup_sizes.append(starters_count)
        out.at[idx, "portiere"] = ";".join(portiere[:1])
        out.at[idx, "difensori"] = ";".join(difensori)
        out.at[idx, "centrocampisti"] = ";".join(centrocampisti)
        out.at[idx, "attaccanti"] = ";".join(attaccanti)
        out.at[idx, "panchina"] = ";".join(_split_players(row.get("panchina")))
        out.at[idx, "modulo"] = _normalize_module(row.get("modulo"))

        if starters_count != 11:
            warnings.append(
                f"Formazioni: riga {int(idx) + 2} con {starters_count} titolari invece di 11."
            )

    if out.empty:
        errors.append("Formazioni: nessuna riga valida trovata.")
    if errors:
        return out, warnings, errors

    out = out.sort_values(["giornata", "team"]).reset_index(drop=True)
    return out, warnings, errors


def _load_status() -> dict[str, object]:
    if not MANUAL_IMPORT_STATUS_PATH.exists():
        return {}
    try:
        raw = json.loads(MANUAL_IMPORT_STATUS_PATH.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def load_manual_import_status() -> dict[str, object]:
    raw = _load_status()
    return {
        "rose": raw.get("rose") if isinstance(raw.get("rose"), dict) else None,
        "quotazioni": raw.get("quotazioni") if isinstance(raw.get("quotazioni"), dict) else None,
        "formazioni": raw.get("formazioni") if isinstance(raw.get("formazioni"), dict) else None,
    }


def _write_source_status(source: ManualImportSource, payload: dict[str, object]) -> None:
    current = _load_status()
    current[source] = payload
    MANUAL_IMPORT_STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = MANUAL_IMPORT_STATUS_PATH.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(MANUAL_IMPORT_STATUS_PATH)


def _write_error_status(
    source: ManualImportSource,
    *,
    original_filename: str,
    stored_path: Path | None,
    activated_path: Path,
    warnings: list[str],
    errors: list[str],
    extra: dict[str, object] | None = None,
) -> dict[str, object]:
    payload = {
        "source": source,
        "status": "error",
        "last_import_at": _status_timestamp(),
        "original_filename": original_filename,
        "stored_path": str(stored_path) if stored_path else "",
        "activated_path": str(activated_path),
        "imported_rows": 0,
        "warnings": warnings,
        "errors": errors,
    }
    if extra:
        payload.update(extra)
    _write_source_status(source, payload)
    return payload


def _backup_active_file(source: ManualImportSource, active_path: Path, stamp: str) -> str:
    if not active_path.exists():
        return ""
    target_dir = MANUAL_BACKUP_DIR / source
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / f"{source}_{stamp}{active_path.suffix or '.csv'}"
    tmp_path = target_path.with_suffix(target_path.suffix + ".tmp")
    shutil.copy2(active_path, tmp_path)
    if not tmp_path.exists() or tmp_path.stat().st_size <= 0:
        tmp_path.unlink(missing_ok=True)
        raise RuntimeError("Backup file attivo non valido")
    tmp_path.replace(target_path)
    return str(target_path)


def save_and_activate_manual_import(
    source: ManualImportSource,
    *,
    original_filename: str,
    fileobj: BinaryIO,
) -> dict[str, object]:
    if source not in ACTIVE_PATHS:
        raise ValueError(f"Source manual import non supportata: {source}")

    clean_original = Path(original_filename or "").name
    suffix = Path(clean_original).suffix.lower()
    active_path = ACTIVE_PATHS[source]
    if suffix not in SUPPORTED_EXTENSIONS:
        errors = ["Formato file non supportato. Usa CSV o XLSX."]
        return _write_error_status(
            source,
            original_filename=clean_original,
            stored_path=None,
            activated_path=active_path,
            warnings=[],
            errors=errors,
        )

    stamp = _timestamp_for_filename()
    stored_dir = MANUAL_INCOMING_DIR / source
    stored_dir.mkdir(parents=True, exist_ok=True)
    stored_path = stored_dir / f"{source}_{stamp}{suffix}"

    with stored_path.open("wb") as handle:
        shutil.copyfileobj(fileobj, handle)

    warnings: list[str] = []
    errors: list[str] = []
    extra: dict[str, object] = {}
    try:
        if source == "rose":
            normalized, warnings, errors = _validate_rose(stored_path)
        elif source == "quotazioni":
            normalized, warnings, errors = _validate_quotazioni(stored_path)
        else:
            normalized, warnings, errors = _validate_formazioni(stored_path)
    except Exception as exc:
        normalized = pd.DataFrame()
        errors = [str(exc)]

    if source == "formazioni" and not normalized.empty:
        rounds_detected = sorted(
            {
                int(value)
                for value in pd.to_numeric(normalized["giornata"], errors="coerce").dropna().astype(int).tolist()
                if int(value) > 0
            }
        )
        teams_detected = sorted({_clean_text(value) for value in normalized["team"].tolist() if _clean_text(value)})
        extra = {
            "rounds_detected": rounds_detected,
            "teams_detected": teams_detected,
        }

    if errors:
        return _write_error_status(
            source,
            original_filename=clean_original,
            stored_path=stored_path,
            activated_path=active_path,
            warnings=warnings,
            errors=errors,
            extra=extra,
        )

    backup_path = _backup_active_file(source, active_path, stamp)
    active_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_active = active_path.with_suffix(active_path.suffix + ".tmp")
    normalized.to_csv(tmp_active, index=False, encoding="utf-8")
    if not tmp_active.exists() or tmp_active.stat().st_size <= 0:
        tmp_active.unlink(missing_ok=True)
        raise RuntimeError("File normalizzato non valido")
    tmp_active.replace(active_path)

    payload = {
        "source": source,
        "status": "ok",
        "last_import_at": _status_timestamp(),
        "original_filename": clean_original,
        "stored_path": str(stored_path),
        "activated_path": str(active_path),
        "backup_path": backup_path,
        "imported_rows": int(len(normalized)),
        "warnings": warnings,
        "errors": [],
    }
    if extra:
        payload.update(extra)
    _write_source_status(source, payload)
    return payload
