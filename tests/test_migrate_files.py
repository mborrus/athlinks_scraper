import json
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dashboard"))

import migrate_files  # noqa: E402
import storage  # noqa: E402


def _row(name, event_id="e1"):
    return {"Source": "athlinks", "Race Group": "g1", "Event ID": event_id, "Event Name": "Trot",
            "Event Date": "2023-11-23", "Race Type": "5K", "Name": name, "Time": "20:00", "Pace": "6:26"}


def test_main_imports_files_and_metadata_into_local_store(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)
    monkeypatch.setattr(migrate_files, "_secrets_from_toml", lambda: {})
    pd.DataFrame([_row("A"), _row("B")]).to_parquet(tmp_path / "scraped_athlinks_g1_2023_e1.parquet", index=False)
    (tmp_path / "event_metadata.json").write_text(json.dumps({"g1": "My Trot"}))

    assert migrate_files.main(["--data-dir", str(tmp_path)]) == 0

    out = capsys.readouterr().out
    assert "scraped_athlinks_g1_2023_e1.parquet: 2 rows" in out
    assert "event_metadata.json: 1 display names" in out
    assert "motherduck_token" not in out
    con = storage.open_store(str(tmp_path / "results.duckdb"))
    assert con.execute("SELECT COUNT(*) FROM results").fetchone()[0] == 2
    assert storage.load_event_metadata(con) == {"g1": "My Trot"}
    con.close()


def test_main_exits_nonzero_on_bad_file_or_bad_metadata(tmp_path, monkeypatch):
    monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)
    monkeypatch.setattr(migrate_files, "_secrets_from_toml", lambda: {})
    (tmp_path / "broken.parquet").write_bytes(b"nope")
    assert migrate_files.main(["--data-dir", str(tmp_path)]) == 1

    for f in tmp_path.iterdir():
        f.unlink()
    (tmp_path / "event_metadata.json").write_text("not json")
    assert migrate_files.main(["--data-dir", str(tmp_path)]) == 1
