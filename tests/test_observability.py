from __future__ import annotations

import json
import threading
import time

from fastapi.testclient import TestClient

from mvp_orbit.client.runtime import ClientRuntime
from mvp_orbit.client.service import ClientService, status_file_path
from mvp_orbit.core.models import ClientEvent
from mvp_orbit.hub.app import create_app
from mvp_orbit.hub.store import HubStore


def _build_client(tmp_path):
    store = HubStore(tmp_path / "hub.sqlite3", tmp_path / "objects")
    app = create_app(store=store)
    return TestClient(app), store


def _join(client: TestClient, alias: str) -> dict:
    payload = client.post("/api/join", json={"alias": alias, "channel": "test-channel"}).json()
    assert payload["status"] == "approved"
    return payload


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def test_join_request_prompt_never_blocks_dispatch(tmp_path):
    release = threading.Event()
    prompted = threading.Event()

    def blocking_prompt(payload: dict) -> bool | None:
        prompted.set()
        release.wait(timeout=10)
        return None

    service = ClientService(
        client_id="client-a",
        hub_url="",
        runtime=ClientRuntime(client_id="client-a", base_workspace=tmp_path / "ws"),
        member_token="tok",
        join_request_prompt=blocking_prompt,
    )
    start = time.monotonic()
    service._dispatch_event(None, "join.request", {"request_id": "join-1", "alias": "b"})
    dispatch_time = time.monotonic() - start
    assert dispatch_time < 0.5  # the consume loop must never wait on a human
    assert prompted.wait(timeout=5)

    # A redelivered event for the same request must not double-prompt.
    service._dispatch_event(None, "join.request", {"request_id": "join-1", "alias": "b"})
    assert "join-1" in service._join_prompts_active
    release.set()
    deadline = time.time() + 5
    while "join-1" in service._join_prompts_active and time.time() < deadline:
        time.sleep(0.02)
    assert "join-1" not in service._join_prompts_active


def test_heartbeat_reports_stream_health_to_hub(tmp_path):
    client, store = _build_client(tmp_path)
    alice = _join(client, "client-a")
    store.register_client("client-a", alice["channel_id"])

    store.apply_client_events("client-a", [ClientEvent(kind="client.heartbeat", payload={"stream_connected": False})])
    peers = client.get("/api/peers", headers=_auth(alice["member_token"])).json()
    assert peers[0]["stream_connected"] is False

    store.apply_client_events("client-a", [ClientEvent(kind="client.heartbeat", payload={"stream_connected": True})])
    peers = client.get("/api/peers", headers=_auth(alice["member_token"])).json()
    assert peers[0]["stream_connected"] is True

    # Legacy clients that send no flag do not clobber the stored value.
    store.apply_client_events("client-a", [ClientEvent(kind="client.heartbeat", payload={})])
    peers = client.get("/api/peers", headers=_auth(alice["member_token"])).json()
    assert peers[0]["stream_connected"] is True


def test_status_file_is_written_and_readable(tmp_path, monkeypatch):
    monkeypatch.setenv("ORBIT_STATE_DIR", str(tmp_path / "state"))
    service = ClientService(
        client_id="client-a",
        hub_url="http://hub.example",
        runtime=ClientRuntime(client_id="client-a", base_workspace=tmp_path / "ws"),
        member_token="tok",
    )
    service._write_status_file()
    path = status_file_path("client-a")
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["client_id"] == "client-a"
    assert payload["hub_url"] == "http://hub.example"
    assert payload["stream_connected"] is False
    assert payload["pid"] > 0
    assert str(tmp_path / "ws") in payload["workspace"]


def test_file_results_report_absolute_paths(tmp_path):
    runtime = ClientRuntime(client_id="client-a", base_workspace=tmp_path / "ws")
    import base64

    result = runtime.handle_file_push(
        transfer_id="t1",
        remote_path="inbox/x.txt",
        data_b64=base64.b64encode(b"hi").decode(),
        max_bytes=100,
    )
    assert result.status.value == "succeeded"
    assert result.remote_path.startswith("/")
    assert result.remote_path.endswith("inbox/x.txt")

    pulled = runtime.handle_file_pull(transfer_id="t2", remote_path="inbox/x.txt", max_bytes=100)
    assert pulled.remote_path == result.remote_path


def test_orbit_stop_terminates_daemon_and_handles_stale_pidfile(tmp_path, monkeypatch, capsys):
    import subprocess

    from mvp_orbit.cli import main as cli_main
    from mvp_orbit.config import ClientConfig, OrbitConfig, save_config

    monkeypatch.setenv("ORBIT_STATE_DIR", str(tmp_path / "state"))
    config_path = tmp_path / "config.toml"
    save_config(OrbitConfig(client=ClientConfig(id="me")), config_path)
    monkeypatch.setenv("ORBIT_CONFIG", str(config_path))

    # No pidfile yet.
    assert cli_main.main(["stop"]) == 1
    assert "nothing to stop" in capsys.readouterr().out

    # A live fake daemon gets terminated. Reap it concurrently: a real
    # daemon is not our child, but this sleeper is, and an unreaped zombie
    # would still answer kill -0.
    proc = subprocess.Popen(["sleep", "60"])
    reaper = threading.Thread(target=proc.wait, daemon=True)
    reaper.start()
    (tmp_path / "state").mkdir(exist_ok=True)
    pidfile = tmp_path / "state" / "daemon-me.pid"
    pidfile.write_text(str(proc.pid))
    assert cli_main.main(["stop"]) == 0
    reaper.join(timeout=5)
    assert proc.returncode != 0  # SIGTERM'd

    # Stale pidfile (process already gone) is cleaned up.
    pidfile.write_text(str(proc.pid))
    assert cli_main.main(["stop"]) == 0
    assert not pidfile.exists()
