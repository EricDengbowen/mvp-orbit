from __future__ import annotations

import sys
import time
from datetime import timedelta

import pytest
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient

from mvp_orbit.client.runtime import ClientRuntime
from mvp_orbit.client.service import ClientService, StreamFailureLimitError
from mvp_orbit.hub.app import create_app
from mvp_orbit.hub.store import HubStore
from mvp_orbit.core.models import utc_now


def _build_client(tmp_path):
    store = HubStore(tmp_path / "hub.sqlite3", tmp_path / "objects")
    app = create_app(store=store)
    return TestClient(app), store


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _join(client: TestClient, alias: str, channel: str = "team", approver_token: str | None = None) -> dict:
    payload = client.post("/api/join", json={"alias": alias, "channel": channel}).json()
    if payload["status"] == "pending":
        assert approver_token is not None
        client.post(f"/api/join-requests/{payload['request_id']}/approve", headers=_auth(approver_token))
        payload = client.get(f"/api/join-requests/{payload['request_id']}").json()
    assert payload["status"] == "approved"
    return payload


def test_rejoin_of_existing_alias_requires_approval(tmp_path):
    client, _ = _build_client(tmp_path)
    alice = _join(client, "alice")
    _join(client, "bob", approver_token=alice["member_token"])

    # With other members present, claiming an existing member's alias must NOT
    # hand out a token — /api/join is unauthenticated and 'alice' is the admin.
    rejoin = client.post("/api/join", json={"alias": "alice", "channel": "team"}).json()
    assert rejoin["status"] == "pending"
    assert rejoin["member_token"] is None

    # After approval by an existing member the returning alias keeps its role.
    client.post(f"/api/join-requests/{rejoin['request_id']}/approve", headers=_auth(alice["member_token"]))
    approved = client.get(f"/api/join-requests/{rejoin['request_id']}").json()
    assert approved["status"] == "approved"
    assert approved["member_token"]
    members = client.get("/api/members", headers=_auth(approved["member_token"])).json()
    assert ("alice", "admin") in [(m["alias"], m["role"]) for m in members]


def test_evicting_member_also_revokes_legacy_tokens(tmp_path):
    client, store = _build_client(tmp_path)
    alice = _join(client, "alice")
    bob = _join(client, "bob", approver_token=alice["member_token"])

    # Simulate bob holding a pre-v0.7 token (no alias binding).
    legacy_token = "legacy-secret"
    with store._lock, store._conn:
        store._conn.execute(
            "INSERT INTO member_tokens (token_hash, channel_id, created_at, expires_at, revoked_at, alias) VALUES (?, ?, ?, ?, NULL, NULL)",
            (
                HubStore._hash_token(legacy_token),
                alice["channel_id"],
                utc_now().isoformat(),
                (utc_now() + timedelta(days=1)).isoformat(),
            ),
        )
    assert client.get("/api/peers", headers=_auth(legacy_token)).status_code == 200

    removed = client.post("/api/members/bob/remove", headers=_auth(alice["member_token"]))
    assert removed.status_code == 200
    # Both bob's aliased tokens AND all unattributable legacy tokens are dead.
    assert client.get("/api/peers", headers=_auth(bob["member_token"])).status_code == 401
    assert client.get("/api/peers", headers=_auth(legacy_token)).status_code == 401
    # The remover's aliased token still works.
    assert client.get("/api/peers", headers=_auth(alice["member_token"])).status_code == 200


def test_running_command_of_lost_client_is_reaped(tmp_path):
    client, store = _build_client(tmp_path)
    alice = _join(client, "alice")
    store.register_client("worker", alice["channel_id"])

    created = client.post(
        "/api/commands",
        json={"client_id": "worker", "argv": ["sleep", "999"], "working_dir": ".", "timeout_sec": 3600, "env_patch": {}},
        headers=_auth(alice["member_token"]),
    )
    command_id = created.json()["command_id"]
    claimed = client.post(f"/api/commands/{command_id}/claim", headers=_auth(alice["member_token"]))
    assert claimed.status_code == 200

    # Fresh client → not reaped.
    assert store.reap_unclaimed_work(default_claim_timeout_sec=3600, client_lost_after_sec=60) == 0

    with store._lock, store._conn:
        store._conn.execute(
            "UPDATE clients SET last_seen_at = ? WHERE client_id = 'worker'",
            ((utc_now() - timedelta(seconds=120)).isoformat(),),
        )
    assert store.reap_unclaimed_work(default_claim_timeout_sec=3600, client_lost_after_sec=60) == 1

    record = client.get(f"/api/commands/{command_id}", headers=_auth(alice["member_token"])).json()
    assert record["status"] == "failed"
    assert record["failure_code"] == "client_lost"
    events = store.get_command_events(command_id, 0)
    assert events[-1].kind == "command.exit"
    assert events[-1].payload["failure_code"] == "client_lost"


def test_instantly_closing_streams_count_toward_give_up(tmp_path):
    app = FastAPI()

    @app.get("/api/clients/{client_id}/stream")
    def empty_stream(client_id: str):
        return StreamingResponse(iter(()), media_type="text/event-stream")

    service = ClientService(
        client_id="client-a",
        hub_url="",
        runtime=ClientRuntime(client_id="client-a", base_workspace=tmp_path / "ws"),
        member_token="tok",
        max_stream_failures=2,
    )
    start = time.monotonic()
    with pytest.raises(StreamFailureLimitError, match="short-lived"):
        service.run_forever(TestClient(app))
    assert time.monotonic() - start < 20.0


def test_command_request_serialization_omits_none_fields():
    from mvp_orbit.core.models import CommandCreateRequest

    request = CommandCreateRequest(client_id="x", argv=["echo"], timeout_sec=30)
    payload = request.model_dump(mode="json", exclude_none=True)
    assert "claim_timeout_sec" not in payload  # old hubs use extra="forbid"
    assert payload["env_patch"] == {}


def test_sole_member_channel_can_reenroll_itself(tmp_path):
    client, _ = _build_client(tmp_path)
    _join(client, "alice", channel="solo")
    rejoin = client.post("/api/join", json={"alias": "alice", "channel": "solo"}).json()
    assert rejoin["status"] == "approved"  # nobody else could approve; not a lockout
    assert rejoin["member_token"]
    # A different alias still needs approval even on a single-member channel.
    other = client.post("/api/join", json={"alias": "mallory", "channel": "solo"}).json()
    assert other["status"] == "pending"


def test_approved_join_request_token_is_single_use(tmp_path):
    client, _ = _build_client(tmp_path)
    alice = _join(client, "alice")
    pending = client.post("/api/join", json={"alias": "bob", "channel": "team"}).json()
    client.post(f"/api/join-requests/{pending['request_id']}/approve", headers=_auth(alice["member_token"]))

    first = client.get(f"/api/join-requests/{pending['request_id']}").json()
    assert first["status"] == "approved"
    assert first["member_token"]

    replay = client.get(f"/api/join-requests/{pending['request_id']}").json()
    assert replay["status"] == "approved"
    assert replay["member_token"] is None  # the request id is not a reusable credential
