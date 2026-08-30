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
        payload = client.get(f"/api/join-requests/{payload['request_id']}", params={"secret": payload["claim_secret"]}).json()
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
    approved = client.get(f"/api/join-requests/{rejoin['request_id']}", params={"secret": rejoin["claim_secret"]}).json()
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


def test_approved_join_request_token_needs_claim_secret(tmp_path):
    client, _ = _build_client(tmp_path)
    alice = _join(client, "alice")
    pending = client.post("/api/join", json={"alias": "bob", "channel": "team"}).json()
    client.post(f"/api/join-requests/{pending['request_id']}/approve", headers=_auth(alice["member_token"]))

    # Without the claim secret (e.g. a request id scraped from logs): no token.
    scraped = client.get(f"/api/join-requests/{pending['request_id']}").json()
    assert scraped["status"] == "approved"
    assert scraped["member_token"] is None

    # The requester (who holds the secret) can poll and even retry safely.
    first = client.get(f"/api/join-requests/{pending['request_id']}", params={"secret": pending["claim_secret"]}).json()
    assert first["member_token"]
    retry = client.get(f"/api/join-requests/{pending['request_id']}", params={"secret": pending["claim_secret"]}).json()
    assert retry["member_token"]  # a lost response is recoverable
    assert client.get("/api/peers", headers={"Authorization": f"Bearer {retry['member_token']}"}).status_code == 200

    wrong = client.get(f"/api/join-requests/{pending['request_id']}", params={"secret": "nope"}).json()
    assert wrong["member_token"] is None


def test_reconnect_reconcile_fails_stale_running_work(tmp_path):
    from mvp_orbit.core.models import ClientEvent

    client, store = _build_client(tmp_path)
    alice = _join(client, "alice")
    store.register_client("worker", alice["channel_id"])

    created = client.post(
        "/api/commands",
        json={"client_id": "worker", "argv": ["sleep", "999"], "working_dir": ".", "timeout_sec": 3600, "env_patch": {}},
        headers=_auth(alice["member_token"]),
    )
    stale_id = created.json()["command_id"]
    client.post(f"/api/commands/{stale_id}/claim", headers=_auth(alice["member_token"]))

    live = client.post(
        "/api/commands",
        json={"client_id": "worker", "argv": ["sleep", "999"], "working_dir": ".", "timeout_sec": 3600, "env_patch": {}},
        headers=_auth(alice["member_token"]),
    )
    live_id = live.json()["command_id"]
    client.post(f"/api/commands/{live_id}/claim", headers=_auth(alice["member_token"]))

    # The restarted client reconciles: it only knows about the live command.
    store.apply_client_events("worker", [ClientEvent(kind="client.reconcile", payload={"active_command_ids": [live_id]})])

    stale = client.get(f"/api/commands/{stale_id}", headers=_auth(alice["member_token"])).json()
    assert stale["status"] == "failed"
    assert stale["failure_code"] == "client_restarted"
    events = store.get_command_events(stale_id, 0)
    assert events[-1].kind == "command.exit"

    still_live = client.get(f"/api/commands/{live_id}", headers=_auth(alice["member_token"])).json()
    assert still_live["status"] == "running"  # genuinely running work is untouched


def test_terminal_retry_stops_on_shutdown(tmp_path):
    from mvp_orbit.core.models import ClientEvent
    import threading

    service = ClientService(
        client_id="client-a",
        hub_url="http://127.0.0.1:1",  # unreachable
        runtime=ClientRuntime(client_id="client-a", base_workspace=tmp_path / "ws"),
        member_token="tok",
    )
    import httpx as _httpx

    done = threading.Event()

    def worker():
        with _httpx.Client(timeout=1.0) as c:
            service._post_terminal_event(c, ClientEvent(kind="command.exit", payload={"command_id": "x", "status": "failed"}), what="command", key="x")
        done.set()

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    time.sleep(1.5)  # let it fail at least once and enter its wait
    service._shutdown.set()
    assert done.wait(timeout=10.0)  # the retry loop must exit promptly on shutdown


def test_join_reuses_valid_saved_credentials(tmp_path, monkeypatch, capsys):
    from datetime import timedelta as _td

    from mvp_orbit.cli import main as cli_main
    from mvp_orbit.config import AuthConfig, ClientConfig, HubConfig, OrbitConfig, save_config

    config_path = tmp_path / "config.toml"
    save_config(
        OrbitConfig(
            hub=HubConfig(url="http://hub.example"),
            auth=AuthConfig(member_token="tok", expires_at=utc_now() + _td(days=1)),
            client=ClientConfig(id="me"),
        ),
        config_path,
    )
    monkeypatch.setenv("ORBIT_CONFIG", str(config_path))

    def _no_network(*a, **k):
        raise AssertionError("join must not hit the network when saved credentials are valid")

    monkeypatch.setattr(cli_main, "_post_join_with_retry", _no_network)
    assert cli_main.main(["join", "--alias", "me", "--channel", "team", "--no-start"]) == 0
    out = capsys.readouterr().out
    assert "already-enrolled" in out
