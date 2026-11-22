import json
import pytest
from fastapi.testclient import TestClient
from audience_experience.api.historical import load_historical
from audience_experience.api.app import create_app


def valid():
    return {"schema_version": "i3-historical-v1", "status": "PASS", "release": "r01",
            "exported_at": "2026-11-21T22:00:00Z",
            "source_namespace": "workspace.twitch_lakehouse",
            "metrics": {"bronze_rows": 4, "silver_rows": 3, "rejected_rows": 1,
                        "gold_streamers": 2, "estimated_minutes": 30,
                        "top10_share": 1.0, "top100_share": 1.0},
            "ranking": [{"streamer_username": "fixture", "interactions": 2}],
            "duration": [{"duration_bucket": i, "interactions": 3 if i == 0 else 0}
                         for i in range(5)]}


def test_valid_export(tmp_path):
    target = tmp_path / "snapshot.json"
    target.write_text(json.dumps(valid()), encoding="utf-8")
    assert load_historical(target)["metrics"]["silver_rows"] == 3


@pytest.mark.parametrize("change", [
    lambda d: d.update(status="PENDING"),
    lambda d: d["metrics"].update(top10_share=1.5),
    lambda d: d["metrics"].update(silver_rows=10),
    lambda d: d["duration"][0].update(interactions=2),
    lambda d: d.update(duration=[]),
])
def test_invalid_export_rejected(tmp_path, change):
    payload = valid()
    change(payload)
    target = tmp_path / "snapshot.json"
    target.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError):
        load_historical(target)


def test_route_missing_is_503(monkeypatch, tmp_path):
    monkeypatch.setenv("I3_HISTORICAL_PATH", str(tmp_path / "none.json"))
    with TestClient(create_app(tmp_path / "missing.sqlite")) as client:
        response = client.get("/api/v1/historical")
        assert response.status_code == 503
        assert response.json()["detail"]["code"] == "historical_unavailable"


def test_route_reads_export_without_sqlite(monkeypatch, tmp_path):
    target = tmp_path / "historical.json"
    target.write_text(json.dumps(valid()), encoding="utf-8")
    monkeypatch.setenv("I3_HISTORICAL_PATH", str(target))
    with TestClient(create_app(tmp_path / "missing.sqlite")) as client:
        response = client.get("/api/v1/historical")
        assert response.status_code == 200
        assert response.json()["metrics"]["silver_rows"] == 3
        assert not (tmp_path / "missing.sqlite").exists()
