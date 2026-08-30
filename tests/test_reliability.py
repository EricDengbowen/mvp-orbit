from __future__ import annotations

import argparse
import sys
import time

import pytest
from fastapi.testclient import TestClient

from mvp_orbit.cli.main import _extract_trailing_exec_options
from mvp_orbit.client.runtime import ClientRuntime
from mvp_orbit.client.service import ClientService, StreamFailureLimitError
from mvp_orbit.core.models import ClientEvent, ClientEventsRequest, CommandLease, utc_now
from mvp_orbit.hub.app import create_app
from mvp_orbit.hub.store import HubStore


def _build_client(tmp_path):
    store = HubStore(tmp_path / "hub.sqlite3", tmp_path / "objects")
    app = create_app(store=store)
    return TestClient(app), store


def _join(client: TestClient, alias: str, channel: str = "test-channel") -> dict:
    response = client.post("/api/join", json={"alias": alias, "channel": channel})
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "approved"
    return payload


def _auth(member_token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {member_token}"}


def _exec_args(command_argv: list[str]) -> argparse.Namespace:
    return argparse.Namespace(
        command_argv=command_argv,
        shell=False,
        timeout_sec=3600,
        claim_timeout=None,
        working_dir=".",
    )


# --- exec option extraction (the "--shell after peer name" footgun) ----------


def test_exec_options_after_peer_are_recognized():
    args = _exec_args(["--shell", "echo hi | wc -c"])
    _extract_trailing_exec_options(args)
    assert args.shell is True
    assert args.command_argv == ["echo hi | wc -c"]


def test_exec_valued_options_after_peer_are_recognized():
    args = _exec_args(["--timeout-sec", "5", "--claim-timeout=7", "--working-dir", "sub", "--", "sleep", "1"])
    _extract_trailing_exec_options(args)
    assert args.timeout_sec == 5
    assert args.claim_timeout == 7
    assert args.working_dir == "sub"
    assert args.command_argv == ["--", "sleep", "1"]


def test_exec_unknown_option_after_peer_fails_loudly():
    args = _exec_args(["--bogus", "echo", "hi"])
    with pytest.raises(SystemExit):
        _extract_trailing_exec_options(args)


def test_exec_literal_args_after_separator_untouched():
    args = _exec_args(["--", "grep", "--shell", "file.txt"])
    _extract_trailing_exec_options(args)
    assert args.shell is False
    assert args.command_argv == ["--", "grep", "--shell", "file.txt"]


# --- runtime: spawn failures and stdin ---------------------------------------


def _run_lease(runtime: ClientRuntime, argv: list[str], timeout_sec: int = 30):
    outputs: list[tuple[str, str]] = []
    outcome = runtime.handle_command(
        CommandLease(command_id="cmd-x", client_id="client-a", argv=argv, env_patch={}, timeout_sec=timeout_sec, working_dir="."),
        on_started=lambda: None,
        append_output=lambda stream, data: outputs.append((stream, data)),
        should_cancel=lambda: False,
    )
    return outcome, outputs


def test_missing_binary_returns_terminal_outcome(tmp_path):
    runtime = ClientRuntime(client_id="client-a", base_workspace=tmp_path / "ws")
    outcome, outputs = _run_lease(runtime, ["definitely-not-a-real-binary-xyz"])
    assert outcome.status.value == "failed"
    assert outcome.failure_code == "spawn_failed"
    assert outcome.exit_code == 127
    stderr = "".join(data for stream, data in outputs if stream == "stderr")
    assert "cannot start" in stderr


def test_command_stdin_is_devnull_not_inherited_tty(tmp_path):
    runtime = ClientRuntime(client_id="client-a", base_workspace=tmp_path / "ws")
    start = time.monotonic()
    outcome, outputs = _run_lease(
        runtime,
        [sys.executable, "-c", "import sys; print(sys.stdin.isatty()); print(repr(sys.stdin.read()))"],
    )
    elapsed = time.monotonic() - start
    stdout = "".join(data for stream, data in outputs if stream == "stdout")
    assert outcome.status.value == "succeeded"
    assert "False" in stdout
    assert "''" in stdout  # immediate EOF, no waiting on an inherited terminal
    assert elapsed < 5.0


def test_crashed_runtime_still_reports_terminal_status(tmp_path):
    client, store = _build_client(tmp_path)
    alice = _join(client, "client-a")
    store.register_client("client-a", alice["channel_id"])
    created = client.post(
        "/api/commands",
        json={"client_id": "client-a", "argv": ["x"], "working_dir": ".", "timeout_sec": 30, "env_patch": {}},
        headers=_auth(alice["member_token"]),
    )
    command_id = created.json()["command_id"]

    class ExplodingRuntime:
        def handle_command(self, *a, **k):
            raise ValueError("boom")

    service = ClientService(
        client_id="client-a",
        hub_url="",
        runtime=ExplodingRuntime(),
        member_token=alice["member_token"],
    )
    service._run_command(client, command_id, __import__("threading").Event())

    record = client.get(f"/api/commands/{command_id}", headers=_auth(alice["member_token"])).json()
    assert record["status"] == "failed"
    assert record["failure_code"].startswith("runtime_error:")


# --- client stream failure policy --------------------------------------------


def test_run_forever_gives_up_after_max_stream_failures(tmp_path):
    runtime = ClientRuntime(client_id="client-a", base_workspace=tmp_path / "ws")
    service = ClientService(
        client_id="client-a",
        hub_url="http://127.0.0.1:1",  # nothing listens here
        runtime=runtime,
        member_token="tok",
        max_stream_failures=2,
    )
    start = time.monotonic()
    with pytest.raises(StreamFailureLimitError):
        service.run_forever()
    assert time.monotonic() - start < 30.0


def test_run_forever_rejected_token_gives_reenroll_guidance(tmp_path):
    client, store = _build_client(tmp_path)
    _join(client, "client-a")
    with store._lock, store._conn:
        store._conn.execute("UPDATE member_tokens SET revoked_at = ?", (utc_now().isoformat(),))
    runtime = ClientRuntime(client_id="client-a", base_workspace=tmp_path / "ws")
    service = ClientService(client_id="client-a", hub_url="", runtime=runtime, member_token="revoked")
    with pytest.raises(RuntimeError, match="re-enroll"):
        service.run_forever(client)


# --- hub: unclaimed work reaper ----------------------------------------------


def test_unclaimed_command_is_failed_with_terminal_event(tmp_path):
    client, store = _build_client(tmp_path)
    alice = _join(client, "client-a")
    store.register_client("client-b", alice["channel_id"])

    created = client.post(
        "/api/commands",
        json={
            "client_id": "client-b",
            "argv": ["echo", "hi"],
            "working_dir": ".",
            "timeout_sec": 30,
            "env_patch": {},
            "claim_timeout_sec": 1,
        },
        headers=_auth(alice["member_token"]),
    )
    command_id = created.json()["command_id"]
    assert store.reap_unclaimed_work(default_claim_timeout_sec=3600) == 0  # not yet due
    time.sleep(1.1)
    assert store.reap_unclaimed_work(default_claim_timeout_sec=3600) == 1

    record = client.get(f"/api/commands/{command_id}", headers=_auth(alice["member_token"])).json()
    assert record["status"] == "failed"
    assert record["failure_code"] == "unclaimed"
    events = store.get_command_events(command_id, 0)
    assert events[-1].kind == "command.exit"
    assert events[-1].payload["failure_code"] == "unclaimed"

    # A peer that comes back later can neither claim it nor resurrect it.
    claimed = client.post(f"/api/commands/{command_id}/claim", headers=_auth(alice["member_token"]))
    assert claimed.status_code == 409
    client.post(
        f"/api/clients/client-b/events",
        json=ClientEventsRequest(
            events=[ClientEvent(kind="command.exit", payload={"command_id": command_id, "status": "succeeded", "exit_code": 0})]
        ).model_dump(mode="json"),
        headers=_auth(alice["member_token"]),
    )
    record = client.get(f"/api/commands/{command_id}", headers=_auth(alice["member_token"])).json()
    assert record["status"] == "failed"
    assert record["failure_code"] == "unclaimed"


def test_unclaimed_shell_and_file_are_reaped(tmp_path):
    client, store = _build_client(tmp_path)
    alice = _join(client, "client-a")
    store.register_client("client-b", alice["channel_id"])

    shell = client.post("/api/shells", json={"client_id": "client-b"}, headers=_auth(alice["member_token"])).json()
    import base64

    push = client.post(
        "/api/files/push",
        json={"client_id": "client-b", "remote_path": "x.txt", "data_b64": base64.b64encode(b"hi").decode(), "max_bytes": 1024},
        headers=_auth(alice["member_token"]),
    ).json()

    time.sleep(0.6)
    assert store.reap_unclaimed_work(default_claim_timeout_sec=0.5) == 2

    assert client.get(f"/api/shells/{shell['session_id']}", headers=_auth(alice["member_token"])).json()["status"] == "failed"
    transfer = client.get(f"/api/files/{push['transfer_id']}", headers=_auth(alice["member_token"])).json()
    assert transfer["status"] == "failed"
    assert transfer["failure_code"] == "unclaimed"
    file_events = store.get_file_events(push["transfer_id"], 0)
    assert file_events[-1].kind == "file.result"
    assert file_events[-1].payload["failure_code"] == "unclaimed"


def test_env_refresh_adopts_new_variables_and_protects_orbit_vars():
    import os

    from mvp_orbit.cli.main import _refresh_environment

    os.environ["ORBIT_TEST_PROTECTED"] = "original"
    try:
        _refresh_environment("export ORBIT_TEST_PROTECTED=hacked; export ORBIT_REFRESH_PROBE_XYZ=fresh-value; echo noise")
        assert "ORBIT_REFRESH_PROBE_XYZ" not in os.environ  # ORBIT_ prefix is never adopted
        assert os.environ["ORBIT_TEST_PROTECTED"] == "original"
        _refresh_environment("export REFRESH_PROBE_PLAIN_XYZ=fresh-value")
        assert os.environ["REFRESH_PROBE_PLAIN_XYZ"] == "fresh-value"
    finally:
        os.environ.pop("ORBIT_TEST_PROTECTED", None)
        os.environ.pop("REFRESH_PROBE_PLAIN_XYZ", None)


def test_env_refresh_failure_keeps_environment():
    import os

    from mvp_orbit.cli.main import _refresh_environment

    os.environ["REFRESH_KEEP_XYZ"] = "keep"
    try:
        _refresh_environment("exit 7")  # failing refresh must change nothing
        assert os.environ["REFRESH_KEEP_XYZ"] == "keep"
    finally:
        os.environ.pop("REFRESH_KEEP_XYZ", None)
