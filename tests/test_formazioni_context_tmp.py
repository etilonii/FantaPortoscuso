import json
from pathlib import Path

from apps.api.app.routes import data as d


def test_extract_formazioni_tmp_entries_from_html_parses_fields():
    html = """
    <script>
      __.s('tmp', "25|1771285877559|53267|25|2|1,2,3");
    </script>
    """

    entries = d._extract_formazioni_tmp_entries_from_html(html)
    assert len(entries) == 1
    assert entries[0]["round"] == 25
    assert entries[0]["timestamp"] == 1771285877559
    assert entries[0]["competition_id"] == 53267
    assert entries[0]["last_comp_round"] == 25
    assert entries[0]["competition_type"] == 2
    assert entries[0]["team_ids"] == [1, 2, 3]


def test_refresh_formazioni_context_html_uses_tmp_fallback(monkeypatch, tmp_path: Path):
    html_content = """
    <script>
      var data = { authAppKey: "APPKEY" };
      var league = { alias: "fantaportoscuso" };
      __.s('lt', __.dp('eyJzdGF0ZSI6MSwic3VjY2VzcyI6dHJ1ZSwiZGF0YSI6W10sImVycm9yX21zZ3MiOm51bGwsInRva2VuIjoiIiwidXBkYXRlIjp0cnVlfQ=='));
      __.s('tmp', "25|1771285877559|53267|25|2|1,2,3");
    </script>
    """
    html_path = tmp_path / "formazioni_page.html"
    html_path.write_text(html_content, encoding="utf-8")

    written_dir = tmp_path / "tmp_store"
    written_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(d, "REAL_FORMATIONS_TMP_DIR", written_dir)
    monkeypatch.setattr(d, "_context_html_candidates", lambda: [html_path])
    monkeypatch.setattr(d, "LEGHE_ALIAS", None)
    d._FORMAZIONI_REMOTE_REFRESH_CACHE.clear()

    fake_payload = {
        "data": {
            "giornataLega": 25,
            "formazioni": [
                {
                    "giornata": 25,
                    "sq": [
                        {
                            "id": 1,
                            "m": "343;",
                            "pl": [
                                {"id": 10, "i": "10", "n": "Portiere Test", "r": "P"},
                                {"id": 11, "i": "11", "n": "Difensore Test", "r": "D"},
                            ],
                        }
                    ],
                }
            ],
        }
    }

    monkeypatch.setattr(d, "_download_formazioni_pagina_payload", lambda **kwargs: fake_payload)

    out_path = d._refresh_formazioni_appkey_from_context_html(25)
    assert out_path is not None
    assert out_path.exists()
    saved = json.loads(out_path.read_text(encoding="utf-8-sig"))
    assert saved["data"]["giornataLega"] == 25
    assert isinstance(saved["data"]["formazioni"], list)
