"""Hub event streams must stay alive no matter how busy the hub is.

Regression tests for the 2026-09 incident: the keepalive was only sent when
the WHOLE hub had been idle for 5s, and every heartbeat from any client counted
as activity. With a handful of clients online no keepalive was ever sent, every
client and CLI stream hit its 30s read timeout, and clients restarted in a loop.
"""

from __future__ import annotations

import socket
import threading
import time

import httpx
import pytest
import uvicorn

from mvp_orbit.core.models import CommandCreateRequest
from mvp_orbit.hub.app import create_app
from mvp_orbit.hub.store import AliasInUseError, HubStore


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class _Hub:
    def __init__(self, base: str, store: HubStore) -> None:
        self.base = base
        self.store = store

    def join(self, alias: str, channel: str) -> str:
        response = httpx.post(f"{self.base}/api/join", json={"alias": alias, "channel": channel}, timeout=10)
        response.raise_for_status()
        payload = response.json()
        assert payload["status"] == "approved", payload
        return str(payload["member_token"])


@pytest.fixture
def start_hub(tmp_path, monkeypatch):
    """Factory: a real uvicorn hub on a free port (TestClient cannot consume an endless SSE body)."""
    running: list[tuple[uvicorn.Server, threading.Thread]] = []

    def _start(keepalive_sec: float) -> _Hub:
        monkeypatch.setenv("ORBIT_STREAM_KEEPALIVE_SEC", str(keepalive_sec))
        monkeypatch.setenv("ORBIT_CHANNEL_CLEANUP_ENABLED", "0")
        index = len(running)
        store = HubStore(tmp_path / f"hub-{index}.sqlite3", tmp_path / f"objects-{index}")
        port = _free_port()
        config = uvicorn.Config(create_app(store=store), host="127.0.0.1", port=port, log_level="warning", timeout_graceful_shutdown=1)
        server = uvicorn.Server(config)
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        deadline = time.monotonic() + 10
        while not server.started:
            assert time.monotonic() < deadline, "hub did not start"
            time.sleep(0.02)
        running.append((server, thread))
        return _Hub(f"http://127.0.0.1:{port}", store)

    yield _start
    for server, thread in running:
        server.should_exit = True
    for server, thread in running:
        thread.join(timeout=10)


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _read_stream(hub: _Hub, token: str, client_id: str, duration: float, *, max_silence: float) -> list[tuple[float, str]]:
    """Collect (seconds since connect, line) for ``duration`` seconds.

    A silence longer than ``max_silence`` fails the test instead of hanging it:
    that is exactly the read timeout real clients run into.
    """
    lines: list[tuple[float, str]] = []
    timeout = httpx.Timeout(connect=5.0, read=max_silence, write=5.0, pool=5.0)
    with httpx.Client(timeout=timeout) as client:
        with client.stream(
            "GET",
            f"{hub.base}/api/clients/{client_id}/stream",
            headers=_auth(token) | {"Accept": "text/event-stream"},
        ) as response:
            assert response.status_code == 200, response.read()
            start = time.monotonic()
            try:
                for line in response.iter_lines():
                    lines.append((time.monotonic() - start, line))
                    if time.monotonic() - start >= duration:
                        break
            except httpx.ReadTimeout:
                pytest.fail(f"stream went silent for more than {max_silence}s after {len(lines)} lines - keepalive starved")
    return lines


def _keepalives(lines: list[tuple[float, str]]) -> list[float]:
    return [at for at, line in lines if line.startswith(": keepalive")]


def _heartbeat_noise(hub: _Hub, token: str, client_id: str, stop: threading.Event, interval: float) -> None:
    payload = {"events": [{"kind": "client.heartbeat", "payload": {"stream_connected": True}}]}
    with httpx.Client(timeout=5) as client:
        while not stop.is_set():
            client.post(f"{hub.base}/api/clients/{client_id}/events", headers=_auth(token), json=payload)
            stop.wait(interval)


def test_keepalive_is_sent_while_the_hub_is_busy(start_hub):
    hub = start_hub(0.2)
    token = hub.join("busy-client", "busy-channel")
    noisy_token = hub.join("noisy-client", "noisy-channel")
    stop = threading.Event()
    # Far more frequent than the keepalive interval: the hub is never "idle".
    noise = threading.Thread(target=_heartbeat_noise, args=(hub, noisy_token, "noisy-client", stop, 0.03), daemon=True)
    noise.start()
    try:
        lines = _read_stream(hub, token, "busy-client", 1.6, max_silence=1.5)
    finally:
        stop.set()
        noise.join(timeout=5)
    keepalives = _keepalives(lines)
    assert len(keepalives) >= 4, lines
    gaps = [b - a for a, b in zip([0.0, *keepalives], keepalives)]
    assert max(gaps) < 1.0, gaps


def test_keepalive_is_sent_while_the_hub_is_idle(start_hub):
    hub = start_hub(0.2)
    token = hub.join("idle-client", "idle-channel")
    keepalives = _keepalives(_read_stream(hub, token, "idle-client", 1.2, max_silence=1.5))
    assert len(keepalives) >= 3, keepalives


def test_new_work_wakes_the_stream_without_waiting_for_the_keepalive(start_hub):
    # A long keepalive proves delivery comes from the change notification, not from a timer.
    hub = start_hub(30.0)
    token = hub.join("worker", "work-channel")
    # Register the client id before work is addressed to it.
    httpx.post(f"{hub.base}/api/clients/worker/events", headers=_auth(token), json={"events": []}, timeout=10).raise_for_status()
    created: dict[str, float] = {}

    def _create() -> None:
        time.sleep(0.4)
        created["at"] = time.monotonic()
        httpx.post(
            f"{hub.base}/api/commands",
            headers=_auth(token),
            json={"client_id": "worker", "argv": ["echo", "hi"], "env_patch": {}, "timeout_sec": 60, "working_dir": "."},
            timeout=10,
        ).raise_for_status()

    creator = threading.Thread(target=_create, daemon=True)
    creator.start()
    received_at = None
    timeout = httpx.Timeout(connect=5.0, read=5.0, write=5.0, pool=5.0)
    with httpx.Client(timeout=timeout) as client:
        with client.stream("GET", f"{hub.base}/api/clients/worker/stream", headers=_auth(token)) as response:
            assert response.status_code == 200
            for line in response.iter_lines():
                if line.startswith("event: command.start"):
                    received_at = time.monotonic()
                    break
    creator.join(timeout=5)
    assert received_at is not None
    assert received_at - created["at"] < 1.5


def test_many_open_streams_all_stay_alive(start_hub):
    # More streams than asyncio's default executor has threads (at most 32):
    # a stream that parks a thread while it waits would starve the rest.
    hub = start_hub(0.2)
    count = 40
    tokens = [hub.join(f"client-{index}", f"channel-{index}") for index in range(count)]
    results: dict[int, list[float] | str] = {}

    def _reader(index: int) -> None:
        try:
            results[index] = _keepalives(_read_stream(hub, tokens[index], f"client-{index}", 1.5, max_silence=3.0))
        except BaseException as exc:  # pytest.fail raises inside the thread
            results[index] = f"{exc.__class__.__name__}: {exc}"

    threads = [threading.Thread(target=_reader, args=(index,), daemon=True) for index in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    assert len(results) == count
    starved = {index: result for index, result in results.items() if isinstance(result, str) or len(result) < 3}
    assert not starved, starved


def _store_with_client(tmp_path) -> tuple[HubStore, str]:
    store = HubStore(tmp_path / "hub.sqlite3", tmp_path / "objects")
    joined = store.request_channel_join(request_id="join-admin", alias="admin", channel="replay-channel")
    store.register_client("admin", joined.channel_id)
    return store, joined.channel_id


def _command(client_id: str) -> CommandCreateRequest:
    return CommandCreateRequest(client_id=client_id, argv=["echo", "hi"], env_patch={}, timeout_sec=60, working_dir=".")


def test_finished_work_is_not_replayed_to_a_restarted_client(tmp_path):
    store, channel_id = _store_with_client(tmp_path)
    # Two join requests, one settled and one still waiting for a decision.
    store.request_channel_join(request_id="join-settled", alias="settled", channel="replay-channel")
    store.reject_join_request("join-settled", channel_id)
    store.request_channel_join(request_id="join-waiting", alias="waiting", channel="replay-channel")
    # Two commands, one already claimed and one still queued.
    store.create_command("cmd-claimed", channel_id, _command("admin"))
    store.claim_command("cmd-claimed")
    store.create_command("cmd-queued", channel_id, _command("admin"))

    everything = store.get_client_control_events("admin", 0)
    assert [event.kind for event in everything] == ["join.request", "join.request", "command.start", "command.start"]

    live, scanned = store.get_live_client_control_events("admin", 0)
    assert [(event.kind, event.payload.get("request_id") or event.payload.get("command_id")) for event in live] == [
        ("join.request", "join-waiting"),
        ("command.start", "cmd-queued"),
    ]
    assert scanned == everything[-1].event_id
    # Nothing new afterwards, and the cursor does not move backwards.
    assert store.get_live_client_control_events("admin", scanned) == ([], scanned)


def test_cancel_events_are_never_dropped(tmp_path):
    store, channel_id = _store_with_client(tmp_path)
    store.create_command("cmd-running", channel_id, _command("admin"))
    store.claim_command("cmd-running")
    store.cancel_command("cmd-running")
    live, _ = store.get_live_client_control_events("admin", 0)
    assert [event.kind for event in live] == ["command.cancel"]


def test_alias_owned_by_another_channel_is_refused_at_join(tmp_path):
    store, channel_id = _store_with_client(tmp_path)
    with pytest.raises(AliasInUseError):
        store.request_channel_join(request_id="join-squatter", alias="admin", channel="other-channel")
    # The owner itself can still re-enroll in its own channel.
    again = store.request_channel_join(request_id="join-again", alias="admin", channel="replay-channel")
    assert again.channel_id == channel_id
    # A different alias is welcome in the other channel.
    other = store.request_channel_join(request_id="join-other", alias="someone-else", channel="other-channel")
    assert other.status.value == "approved"


def test_alias_collision_is_explained_over_http(start_hub):
    hub = start_hub(5.0)
    token = hub.join("npu-login", "first-channel")
    httpx.post(f"{hub.base}/api/clients/npu-login/events", headers=_auth(token), json={"events": []}, timeout=10).raise_for_status()
    response = httpx.post(f"{hub.base}/api/join", json={"alias": "npu-login", "channel": "second-channel"}, timeout=10)
    assert response.status_code == 409
    assert "unique alias" in response.json()["detail"]
