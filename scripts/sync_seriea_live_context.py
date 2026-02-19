from __future__ import annotations

import argparse
import csv
import html
import re
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

SERIEA_CONTEXT_OUT = DATA_DIR / "incoming" / "manual" / "seriea_context.csv"
FIXTURES_OUT = DATA_DIR / "db" / "fixtures.csv"

CALENDAR_BASE_URL = "https://www.fantacalcio.it/serie-a/calendario"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
)


def _infer_season_slug(now: Optional[datetime] = None) -> str:
    current = now or datetime.now(tz=timezone.utc)
    start_year = current.year if current.month >= 7 else current.year - 1
    return f"{start_year}-{str(start_year + 1)[-2:]}"


def _normalize_space(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _strip_html_tags(value: str) -> str:
    return re.sub(r"<[^>]+>", "", str(value or ""))


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or ""))
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _normalize_team_key(value: object) -> str:
    clean = _strip_accents(_normalize_space(value)).lower()
    clean = clean.replace(".", "").replace("-", " ")
    clean = re.sub(r"\s+", " ", clean).strip()
    compact = clean.replace(" ", "")
    aliases = {
        "hellasverona": "verona",
        "ver": "verona",
        "juv": "juventus",
        "rom": "roma",
        "laz": "lazio",
        "tor": "torino",
        "par": "parma",
        "pis": "pisa",
        "cag": "cagliari",
        "bolognaq": "bologna",
    }
    if compact in aliases:
        return aliases[compact]
    if clean in aliases:
        return aliases[clean]
    return compact


def _safe_int(value: object) -> Optional[int]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return int(float(raw.replace(",", ".")))
    except (TypeError, ValueError):
        return None


def _safe_float(value: object) -> Optional[float]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return float(raw.replace(",", "."))
    except (TypeError, ValueError):
        return None


def _http_get(url: str, *, timeout_seconds: float = 30.0) -> str:
    req = Request(
        str(url),
        headers={
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.7,en;q=0.6",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
    )
    try:
        with urlopen(req, timeout=timeout_seconds) as resp:
            body = resp.read()
            encoding = resp.headers.get_content_charset() or "utf-8"
            try:
                return body.decode(encoding, errors="replace")
            except LookupError:
                return body.decode("utf-8", errors="replace")
    except HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code} fetching {url}") from exc
    except URLError as exc:
        raise RuntimeError(f"Network error fetching {url}: {exc}") from exc


def _extract_selected_round(html_text: str) -> Optional[int]:
    match = re.search(
        r"<option\s+value=['\"](\d{1,2})['\"][^>]*selected[^>]*>",
        html_text,
        flags=re.IGNORECASE,
    )
    if match is not None:
        parsed = _safe_int(match.group(1))
        if parsed is not None and parsed > 0:
            return parsed

    header_match = re.search(
        r"<div class=['\"]matchweek['\"]>\s*(\d{1,2})\s*</div>",
        html_text,
        flags=re.IGNORECASE,
    )
    if header_match is not None:
        parsed = _safe_int(header_match.group(1))
        if parsed is not None and parsed > 0:
            return parsed
    return None


def _extract_standings_rows(html_text: str) -> List[Dict[str, object]]:
    section_match = re.search(
        r"<section id=['\"]classifica['\"][^>]*>.*?<tbody>(.*?)</tbody>",
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if section_match is None:
        return []

    body = section_match.group(1)
    rows: List[Dict[str, object]] = []
    for row_html in re.findall(r"<tr\b[^>]*>(.*?)</tr>", body, flags=re.IGNORECASE | re.DOTALL):
        team_match = re.search(r"data-name=['\"]([^'\"]+)['\"]", row_html, flags=re.IGNORECASE)
        team_name = html.unescape(_normalize_space(team_match.group(1))) if team_match else ""
        if not team_name:
            team_anchor = re.search(
                r"<a[^>]*class=['\"][^'\"]*team-name[^'\"]*['\"][^>]*>(.*?)</a>",
                row_html,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if team_anchor is not None:
                team_name = html.unescape(
                    _normalize_space(_strip_html_tags(team_anchor.group(1)))
                )
        if not team_name:
            continue

        def _cell(class_name: str) -> str:
            m = re.search(
                rf"<td[^>]*class=['\"][^'\"]*{class_name}[^'\"]*['\"][^>]*>\s*([^<]+)",
                row_html,
                flags=re.IGNORECASE | re.DOTALL,
            )
            return _normalize_space(_strip_html_tags(html.unescape(m.group(1)))) if m else ""

        pos = _safe_int(_cell("pos"))
        pts = _safe_int(_cell("points"))
        mp = _safe_int(_cell("played"))
        gf = _safe_int(_cell("goalsscored"))
        ga = _safe_int(_cell("goalsconceded"))
        gd = _safe_int(_cell("goalsdifference"))

        form_tokens = re.findall(
            r"data-value=['\"]([WDL])['\"]",
            row_html,
            flags=re.IGNORECASE,
        )
        last5 = " ".join(token.upper() for token in form_tokens[:5]) if form_tokens else ""

        if pos is None or pts is None or mp is None:
            continue
        if gf is None:
            gf = 0
        if ga is None:
            ga = 0
        if gd is None:
            gd = gf - ga

        ppm = round(float(pts) / float(mp), 2) if mp > 0 else 0.0
        rows.append(
            {
                "Pos": int(pos),
                "Squad": team_name,
                "MP": int(mp),
                "GF": int(gf),
                "GA": int(ga),
                "GD": int(gd),
                "Pts": int(pts),
                "Pts/MP": ppm,
                "Last5": last5,
            }
        )

    rows.sort(key=lambda item: int(item.get("Pos") or 999))
    return rows


def _extract_team_name_from_label(label_html: str) -> str:
    anchor_match = re.search(
        r"<a[^>]*class=['\"][^'\"]*team-name[^'\"]*['\"][^>]*>(.*?)</a>",
        label_html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if anchor_match is None:
        return ""
    return html.unescape(_normalize_space(_strip_html_tags(anchor_match.group(1))))


def _extract_fixture_rows(html_text: str, fallback_round: Optional[int]) -> List[Dict[str, object]]:
    pattern = re.compile(
        r"<div[^>]+class=['\"][^'\"]*match-pill[^'\"]*size-large[^'\"]*['\"][^>]*data-match-status=['\"](?P<status>-?\d+)['\"][^>]*>"
        r"(?P<body>.*?)<meta itemprop=['\"]url['\"] content=['\"](?P<url>[^'\"]+)['\"]\s*/>\s*</div>",
        flags=re.IGNORECASE | re.DOTALL,
    )
    rows: List[Dict[str, object]] = []
    seen_ids: set[int] = set()

    for match in pattern.finditer(html_text):
        body = match.group("body")
        match_url = _normalize_space(html.unescape(match.group("url")))
        match_status = _safe_int(match.group("status")) or 0

        round_match = re.search(
            r"<div class=['\"]matchweek['\"]>\s*(\d{1,2})\s*</div>",
            body,
            flags=re.IGNORECASE,
        )
        round_value = _safe_int(round_match.group(1) if round_match else "")
        if round_value is None:
            round_value = fallback_round
        if round_value is None:
            continue

        labels = re.findall(
            r"<label[^>]+class=['\"][^'\"]*team-(?:home|away)[^'\"]*['\"][^>]*>(.*?)</label>",
            body,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if len(labels) < 2:
            continue

        home_team = _extract_team_name_from_label(labels[0])
        away_team = _extract_team_name_from_label(labels[1])
        if not home_team or not away_team:
            continue

        home_score_match = re.search(
            r"<span class=['\"]score-home['\"]>\s*([^<]+)\s*</span>",
            body,
            flags=re.IGNORECASE,
        )
        away_score_match = re.search(
            r"<span class=['\"]score-away['\"]>\s*([^<]+)\s*</span>",
            body,
            flags=re.IGNORECASE,
        )
        home_score = _safe_int(home_score_match.group(1) if home_score_match else "")
        away_score = _safe_int(away_score_match.group(1) if away_score_match else "")

        start_date_match = re.search(
            r"<meta itemprop=['\"]startDate['\"] content=['\"]([^'\"]+)['\"]",
            body,
            flags=re.IGNORECASE,
        )
        hours_match = re.search(
            r"<span class=['\"]hours['\"]>\s*([^<]+)\s*</span>",
            body,
            flags=re.IGNORECASE,
        )
        start_date = _normalize_space(start_date_match.group(1)) if start_date_match else ""
        kickoff = _normalize_space(hours_match.group(1)) if hours_match else ""
        kickoff_iso = ""
        if start_date:
            kickoff_iso = f"{start_date}T{kickoff}" if kickoff else start_date

        id_match = re.search(r"/(\d+)(?:/[^/]*)?$", match_url)
        match_id = _safe_int(id_match.group(1) if id_match else "")
        if match_id is None:
            continue
        if match_id in seen_ids:
            continue
        seen_ids.add(match_id)

        rows.append(
            {
                "round": int(round_value),
                "match_id": int(match_id),
                "home": home_team,
                "away": away_team,
                "home_score": home_score,
                "away_score": away_score,
                "match_status": int(match_status),
                "kickoff_iso": kickoff_iso,
                "match_url": match_url,
            }
        )

    rows.sort(key=lambda item: (int(item.get("round") or 0), str(item.get("home") or ""), str(item.get("away") or "")))
    return rows


def _write_seriea_context(rows: List[Dict[str, object]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fields = ["Squad", "MP", "GF", "GA", "GD", "Pts", "Pts/MP", "Last5"]
    with out_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def _load_existing_fixture_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    except Exception:
        return []


def _build_fixture_pair_rows(fixtures: List[Dict[str, object]]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for row in fixtures:
        round_value = int(row.get("round") or 0)
        home_team = _normalize_space(row.get("home"))
        away_team = _normalize_space(row.get("away"))
        if round_value <= 0 or not home_team or not away_team:
            continue

        home_score = row.get("home_score")
        away_score = row.get("away_score")
        hs = "" if home_score is None else str(int(home_score))
        aw = "" if away_score is None else str(int(away_score))
        status = str(int(row.get("match_status") or 0))
        kickoff_iso = _normalize_space(row.get("kickoff_iso"))
        match_url = _normalize_space(row.get("match_url"))
        match_id = str(int(row.get("match_id") or 0))

        out.append(
            {
                "round": str(round_value),
                "team": home_team,
                "opponent": away_team,
                "home_away": "H",
                "home_score": hs,
                "away_score": aw,
                "team_score": hs,
                "opponent_score": aw,
                "match_status": status,
                "kickoff_iso": kickoff_iso,
                "match_url": match_url,
                "match_id": match_id,
            }
        )
        out.append(
            {
                "round": str(round_value),
                "team": away_team,
                "opponent": home_team,
                "home_away": "A",
                "home_score": hs,
                "away_score": aw,
                "team_score": aw,
                "opponent_score": hs,
                "match_status": status,
                "kickoff_iso": kickoff_iso,
                "match_url": match_url,
                "match_id": match_id,
            }
        )
    return out


def _merge_fixture_rows(
    existing_rows: List[Dict[str, str]],
    incoming_rows: List[Dict[str, str]],
) -> List[Dict[str, str]]:
    merged: Dict[Tuple[int, str, str, str], Dict[str, str]] = {}

    def _key(row: Dict[str, str]) -> Optional[Tuple[int, str, str, str]]:
        round_value = _safe_int(row.get("round"))
        team = _normalize_team_key(row.get("team"))
        opponent = _normalize_team_key(row.get("opponent"))
        home_away = _normalize_space(row.get("home_away")).upper()
        if round_value is None or round_value <= 0 or not team or not opponent or home_away not in {"H", "A"}:
            return None
        return int(round_value), team, opponent, home_away

    for row in existing_rows:
        k = _key(row)
        if k is None:
            continue
        merged[k] = dict(row)

    for row in incoming_rows:
        k = _key(row)
        if k is None:
            continue
        base = dict(merged.get(k, {}))
        base.update(row)
        merged[k] = base

    out = list(merged.values())
    out.sort(
        key=lambda row: (
            _safe_int(row.get("round")) or 0,
            _normalize_team_key(row.get("team")),
            _normalize_team_key(row.get("opponent")),
            _normalize_space(row.get("home_away")).upper(),
        )
    )
    return out


def _write_fixtures(rows: List[Dict[str, str]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    field_order = [
        "round",
        "team",
        "opponent",
        "home_away",
        "home_score",
        "away_score",
        "team_score",
        "opponent_score",
        "match_status",
        "kickoff_iso",
        "match_url",
        "match_id",
    ]
    extras: List[str] = []
    seen = set(field_order)
    for row in rows:
        for key in row.keys():
            if key in seen:
                continue
            seen.add(key)
            extras.append(key)
    fields = field_order + extras

    with out_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def _write_round_incoming_snapshot(rows: List[Dict[str, object]], data_dir: Path, round_value: int) -> Optional[Path]:
    if round_value <= 0:
        return None
    target = data_dir / "incoming" / "fixtures" / f"fixtures_seriea_round{int(round_value)}.csv"
    target.parent.mkdir(parents=True, exist_ok=True)
    fields = ["round", "home", "away", "home_score", "away_score", "match_status", "kickoff_iso", "match_url", "match_id"]
    with target.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})
    return target


def run(round_value: Optional[int], season_slug: Optional[str]) -> Dict[str, object]:
    resolved_season = _normalize_space(season_slug) or _infer_season_slug()
    target_round = int(round_value) if round_value and int(round_value) > 0 else None

    if target_round is not None:
        url = f"{CALENDAR_BASE_URL}/{int(target_round)}"
    else:
        url = f"{CALENDAR_BASE_URL}"

    html_text = _http_get(url)
    selected_round = _extract_selected_round(html_text)
    effective_round = target_round or selected_round

    standings_rows = _extract_standings_rows(html_text)
    fixture_rows = _extract_fixture_rows(html_text, effective_round)
    if not standings_rows:
        raise RuntimeError("Impossibile estrarre classifica Serie A dalla pagina.")
    if not fixture_rows:
        raise RuntimeError("Impossibile estrarre fixtures Serie A dalla pagina.")

    _write_seriea_context(standings_rows, SERIEA_CONTEXT_OUT)

    existing_fixture_rows = _load_existing_fixture_rows(FIXTURES_OUT)
    merged_rows = _merge_fixture_rows(existing_fixture_rows, _build_fixture_pair_rows(fixture_rows))
    _write_fixtures(merged_rows, FIXTURES_OUT)

    round_snapshot = _write_round_incoming_snapshot(
        fixture_rows,
        DATA_DIR,
        int(effective_round or 0),
    )

    return {
        "ok": True,
        "url": url,
        "season": resolved_season,
        "round": int(effective_round) if effective_round else None,
        "standings_rows": len(standings_rows),
        "fixtures_rows": len(fixture_rows),
        "context_path": str(SERIEA_CONTEXT_OUT),
        "fixtures_path": str(FIXTURES_OUT),
        "incoming_snapshot": str(round_snapshot) if round_snapshot else "",
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync real Serie A table + round fixtures (with score/status) from fantacalcio.it"
    )
    parser.add_argument("--round", type=int, default=None, help="Target round to sync (optional).")
    parser.add_argument("--season", type=str, default=None, help="Season slug (e.g. 2025-26).")
    args = parser.parse_args()

    result = run(args.round, args.season)
    print(
        "[ok] seriea sync"
        f" round={result.get('round')}"
        f" standings={result.get('standings_rows')}"
        f" fixtures={result.get('fixtures_rows')}"
    )
    print(f"[ok] context: {result.get('context_path')}")
    print(f"[ok] fixtures: {result.get('fixtures_path')}")
    if result.get("incoming_snapshot"):
        print(f"[ok] incoming: {result.get('incoming_snapshot')}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[error] {exc}", file=sys.stderr)
        raise
