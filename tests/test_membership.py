from __future__ import annotations

from fastapi.testclient import TestClient

from mvp_orbit.hub.app import create_app
from mvp_orbit.hub.store import HubStore


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
        approved = client.post(f"/api/join-requests/{payload['request_id']}/approve", headers=_auth(approver_token))
        assert approved.status_code == 200
        payload = client.get(f"/api/join-requests/{payload['request_id']}", headers={"X-Orbit-Claim": payload["claim_secret"]}).json()
    assert payload["status"] == "approved"
    return payload


def test_first_member_is_admin_and_members_lists_roles(tmp_path):
    client, _ = _build_client(tmp_path)
    alice = _join(client, "alice")
    _join(client, "bob", approver_token=alice["member_token"])

    members = client.get("/api/members", headers=_auth(alice["member_token"])).json()
    assert [(m["alias"], m["role"]) for m in members] == [("alice", "admin"), ("bob", "member")]


def test_admin_can_remove_member_and_tokens_die_immediately(tmp_path):
    client, store = _build_client(tmp_path)
    alice = _join(client, "alice")
    bob = _join(client, "bob", approver_token=alice["member_token"])
    store.register_client("bob", bob["channel_id"])

    removed = client.post("/api/members/bob/remove", headers=_auth(alice["member_token"]))
    assert removed.status_code == 200
    assert removed.json()["status"] == "removed"

    # Bob's token is dead and his client rows are gone.
    assert client.get("/api/peers", headers=_auth(bob["member_token"])).status_code == 401
    peers = client.get("/api/peers", headers=_auth(alice["member_token"])).json()
    assert all(p["client_id"] != "bob" for p in peers)
    members = client.get("/api/members", headers=_auth(alice["member_token"])).json()
    assert [m["alias"] for m in members] == ["alice"]


def test_non_admin_cannot_remove_or_change_roles(tmp_path):
    client, _ = _build_client(tmp_path)
    alice = _join(client, "alice")
    bob = _join(client, "bob", approver_token=alice["member_token"])
    _join(client, "carol", approver_token=alice["member_token"])

    assert client.post("/api/members/carol/remove", headers=_auth(bob["member_token"])).status_code == 403
    assert client.post("/api/members/carol/role", json={"role": "admin"}, headers=_auth(bob["member_token"])).status_code == 403


def test_admin_cannot_remove_self_and_target_must_exist(tmp_path):
    client, _ = _build_client(tmp_path)
    alice = _join(client, "alice")
    _join(client, "bob", approver_token=alice["member_token"])
    assert client.post("/api/members/alice/remove", headers=_auth(alice["member_token"])).status_code == 409
    assert client.post("/api/members/ghost/remove", headers=_auth(alice["member_token"])).status_code == 404


def test_sole_admin_cannot_leave_until_transfer(tmp_path):
    client, _ = _build_client(tmp_path)
    alice = _join(client, "alice")
    bob = _join(client, "bob", approver_token=alice["member_token"])

    blocked = client.post("/api/members/leave", headers=_auth(alice["member_token"]))
    assert blocked.status_code == 409
    assert "transfer-admin" in blocked.json()["detail"]

    promoted = client.post("/api/members/bob/role", json={"role": "admin"}, headers=_auth(alice["member_token"]))
    assert promoted.status_code == 200
    left = client.post("/api/members/leave", headers=_auth(alice["member_token"]))
    assert left.status_code == 200
    assert left.json()["channel_deleted"] is False
    assert client.get("/api/peers", headers=_auth(alice["member_token"])).status_code == 401

    members = client.get("/api/members", headers=_auth(bob["member_token"])).json()
    assert [(m["alias"], m["role"]) for m in members] == [("bob", "admin")]


def test_last_member_leaving_deletes_channel_and_rejoin_starts_fresh(tmp_path):
    client, _ = _build_client(tmp_path)
    alice = _join(client, "alice", channel="solo")
    left = client.post("/api/members/leave", headers=_auth(alice["member_token"]))
    assert left.status_code == 200
    assert left.json()["channel_deleted"] is True

    fresh = client.post("/api/join", json={"alias": "someone-new", "channel": "solo"}).json()
    assert fresh["status"] == "approved"  # first member again → auto-approved admin


def test_sole_admin_cannot_demote_self(tmp_path):
    client, _ = _build_client(tmp_path)
    alice = _join(client, "alice")
    _join(client, "bob", approver_token=alice["member_token"])
    demote = client.post("/api/members/alice/role", json={"role": "member"}, headers=_auth(alice["member_token"]))
    assert demote.status_code == 409


def test_legacy_token_without_alias_gets_guidance(tmp_path):
    client, store = _build_client(tmp_path)
    alice = _join(client, "alice")
    with store._lock, store._conn:
        store._conn.execute("UPDATE member_tokens SET alias = NULL")
    response = client.post("/api/members/leave", headers=_auth(alice["member_token"]))
    assert response.status_code == 403
    assert "re-enroll" in response.json()["detail"]
    # Everything outside member management still works for legacy tokens.
    assert client.get("/api/peers", headers=_auth(alice["member_token"])).status_code == 200


def test_migration_backfills_first_member_as_admin(tmp_path):
    client, store = _build_client(tmp_path)
    alice = _join(client, "alice")
    _join(client, "bob", approver_token=alice["member_token"])
    # Simulate a pre-role database: everyone is a plain member.
    with store._lock, store._conn:
        store._conn.execute("UPDATE channel_members SET role = 'member'")
    reopened = HubStore(tmp_path / "hub.sqlite3", tmp_path / "objects")
    with reopened._lock:
        rows = reopened._conn.execute("SELECT alias, role FROM channel_members ORDER BY created_at ASC").fetchall()
    assert [(r["alias"], r["role"]) for r in rows] == [("alice", "admin"), ("bob", "member")]


def test_valid_token_can_renew_without_approval(tmp_path):
    client, _ = _build_client(tmp_path)
    alice = _join(client, "alice")

    renewed = client.post("/api/members/renew", headers=_auth(alice["member_token"]))
    assert renewed.status_code == 200
    payload = renewed.json()
    assert payload["alias"] == "alice"
    assert payload["member_token"] != alice["member_token"]
    # Both tokens work: the old one ages out naturally (a lost renewal
    # response must not brick the client).
    assert client.get("/api/peers", headers=_auth(payload["member_token"])).status_code == 200
    assert client.get("/api/peers", headers=_auth(alice["member_token"])).status_code == 200


def test_legacy_token_cannot_renew(tmp_path):
    client, store = _build_client(tmp_path)
    alice = _join(client, "alice")
    with store._lock, store._conn:
        store._conn.execute("UPDATE member_tokens SET alias = NULL")
    response = client.post("/api/members/renew", headers=_auth(alice["member_token"]))
    assert response.status_code == 403
    assert "re-enroll" in response.json()["detail"]


def test_renew_helper_is_noop_when_token_is_fresh(tmp_path, monkeypatch):
    from datetime import timedelta as _td

    from mvp_orbit.cli import main as cli_main
    from mvp_orbit.config import AuthConfig, ClientConfig, HubConfig, OrbitConfig, save_config
    from mvp_orbit.core.models import utc_now

    config_path = tmp_path / "config.toml"
    save_config(
        OrbitConfig(
            hub=HubConfig(url="http://hub.example"),
            auth=AuthConfig(member_token="tok", expires_at=utc_now() + _td(days=6)),
            client=ClientConfig(id="me", channel="team"),
        ),
        config_path,
    )

    def _no_network(*a, **k):
        raise AssertionError("fresh token must not trigger a renewal request")

    monkeypatch.setattr(cli_main.httpx, "Client", _no_network)
    assert cli_main._renew_if_needed(str(config_path)) is False


def test_client_swaps_to_renewed_credentials(tmp_path, monkeypatch):
    from datetime import timedelta as _td

    from mvp_orbit.client.main import _load_renewed_credentials
    from mvp_orbit.config import AuthConfig, ClientConfig, HubConfig, OrbitConfig, save_config
    from mvp_orbit.core.models import utc_now

    config_path = tmp_path / "config.toml"
    monkeypatch.setenv("ORBIT_CONFIG", str(config_path))

    save_config(OrbitConfig(auth=AuthConfig(member_token="fresh", expires_at=utc_now() + _td(days=6))), config_path)
    assert _load_renewed_credentials("old")[0] == "fresh"
    assert _load_renewed_credentials("fresh") is None  # same token: nothing to swap to

    save_config(OrbitConfig(auth=AuthConfig(member_token="stale", expires_at=utc_now() - _td(seconds=1))), config_path)
    assert _load_renewed_credentials("old") is None  # expired on disk: no help
