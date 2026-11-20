"""Fixtures ARTIFICIALES; la API lee el contrato real sin crear tablas."""
import json
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
import pytest
from fastapi.testclient import TestClient
from audience_experience.api.app import create_app
from live_monitor.storage import connect, commit_batch

RUN = "00000000-0000-0000-0000-000000000001"
OTHER = "00000000-0000-0000-0000-000000000002"


@pytest.fixture
def db(tmp_path):
    path = tmp_path / 'fixture.sqlite'
    conn = connect(path)
    with conn:
        conn.execute("INSERT INTO runs VALUES(?,?,?,?)", (RUN, json.dumps({"count":100,"source_name":"ARTIFICIAL.csv"}), "COMPLETED", 2))
        conn.execute("INSERT INTO consumer_status VALUES(1,?,?,?)", ("RUNNING", (datetime.now(timezone.utc)-timedelta(seconds=60)).isoformat(), "fixture"))
        rows=[dict(event_key='a',run_id=RUN,payload_hash='a',valid=True,errors='',streamer_username='alpha',estimated_minutes=20,topic='fixture',partition=0,offset=1),dict(event_key='b',run_id=RUN,payload_hash='b',valid=False,errors='fixture',streamer_username=None,estimated_minutes=None,topic='fixture',partition=0,offset=2)]
        commit_batch(conn,'fixture-pipeline',0,rows,[dict(run_id=RUN,streamer_username='alpha',interactions=1,estimated_minutes=20)],[dict(topic='fixture',partition=0,first_offset=1,last_offset=2,records=2)])
    conn.close()
    return path


def test_snapshot_filtered_and_scopes(db):
    with TestClient(create_app(db)) as client:
        response=client.get('/api/v1/snapshot',params={'run_id':RUN})
        assert response.status_code==200 and response.headers['cache-control']=='no-store'
        data=response.json()
        assert data['metrics']['total']==2
        assert data['metrics']['valid']==1 and data['metrics']['invalid']==1
        assert data['metrics']['estimated_minutes_total']==20
        assert data['consumer']['stale'] is True
        assert data['last_batch']['offsets'][0]['last_offset']==2
        assert data['scopes']['last_batch']=='global'
        assert data['activity'][-1]['cumulative_events']==2


def test_prepared_execution_has_real_empty_result(db):
    with TestClient(create_app(db)) as client:
        data=client.get('/api/v1/snapshot',params={'run_id':OTHER}).json()
        assert data['status']=='empty' and data['metrics']['total']==0
        assert data['activity']==[] and data['streamers']==[]
        assert data['last_batch']['input_rows']==2  # explícitamente global


def test_bad_run_id_rejected(db):
    with TestClient(create_app(db)) as client:
        assert client.get('/api/v1/snapshot',params={'run_id':"' OR 1=1"}).status_code==422


def test_storage_missing_returns_503_without_creating(tmp_path):
    missing=tmp_path/'no-such-dir'/'missing.sqlite'
    with TestClient(create_app(missing)) as client:
        assert client.get('/api/v1/health').json()['read_only'] is True
        response=client.get('/api/v1/snapshot')
        assert response.status_code==503
        assert response.json()['detail']['code']=='storage_unavailable'
    assert not missing.parent.exists()


def test_websocket_matches_http(db):
    with TestClient(create_app(db,interval=0.02)) as client:
        http=client.get('/api/v1/snapshot',params={'run_id':RUN}).json()
        with client.websocket_connect('/api/v1/stream?run_id='+RUN) as socket:
            message=socket.receive_json()
            assert message['type']=='snapshot'
            assert message['data']['metrics']==http['metrics']
            assert message['data']['selection']==http['selection']


def test_websocket_storage_unavailable(tmp_path):
    with TestClient(create_app(tmp_path/'missing.sqlite')) as client:
        with client.websocket_connect('/api/v1/stream') as socket:
            assert socket.receive_json()=={'type':'unavailable','code':'storage_unavailable'}


def test_foreign_websocket_origin_rejected(db):
    from starlette.websockets import WebSocketDisconnect
    with TestClient(create_app(db)) as client:
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect('/api/v1/stream',headers={'origin':'https://foreign.example'}):
                pass


def test_api_has_no_write_routes_and_preserves_database(db):
    before=db.read_bytes()
    with TestClient(create_app(db)) as client:
        assert client.post('/api/v1/snapshot').status_code==405
        assert client.delete('/api/v1/snapshot').status_code==405
        assert client.get('/api/v1/snapshot').status_code==200
    assert db.read_bytes()==before
