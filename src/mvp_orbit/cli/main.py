from __future__ import annotations

import argparse
import base64
import getpass
import json
import os
import re
import select
import signal
import shutil
import subprocess
import sys
import termios
import textwrap
import threading
import time
import tty
from datetime import datetime
from pathlib import Path

import httpx
import questionary

from mvp_orbit.config import OrbitConfig, load_config, save_config
from mvp_orbit.core.models import (
    CommandCreateRequest,
    CommandStatus,
    FilePullRequest,
    FilePushRequest,
    FileTransferStatus,
    JoinRequest,
    JoinRequestStatus,
    ShellResizeRequest,
    ShellSessionCreateRequest,
    ShellSessionStatus,
    utc_now,
)

_SHELL_META_PATTERN = re.compile(r"(?:&&|\|\||[|;<>()$`\n])")
class SetupWizard:
    def __init__(self, title: str, subtitle: str) -> None:
        self.title = title
        self.subtitle = subtitle
        self.width = min(92, shutil.get_terminal_size((92, 24)).columns)
        self.color = sys.stdout.isatty() and os.getenv("TERM", "dumb") != "dumb" and not os.getenv("NO_COLOR")
        self.interactive = sys.stdin.isatty() and sys.stdout.isatty() and os.getenv("TERM", "dumb") != "dumb"
        self.qstyle = questionary.Style(
            [
                ("qmark", "fg:#23b7d9 bold"),
                ("question", "fg:#e8f1f2 bold"),
                ("answer", "fg:#77e0c6 bold"),
                ("pointer", "fg:#ffcf56 bold"),
                ("highlighted", "fg:#ffcf56 bold"),
                ("selected", "fg:#77e0c6"),
                ("separator", "fg:#6a7d89"),
                ("instruction", "fg:#6a7d89"),
                ("text", "fg:#e8f1f2"),
                ("disabled", "fg:#6a7d89 italic"),
            ]
        )
        self._print_banner()

    def _style(self, text: str, code: str) -> str:
        if not self.color:
            return text
        return f"\033[{code}m{text}\033[0m"

    def _accent(self, text: str) -> str:
        return self._style(text, "38;5;45;1")

    def _muted(self, text: str) -> str:
        return self._style(text, "38;5;246")

    def _success(self, text: str) -> str:
        return self._style(text, "38;5;84;1")

    def _warning(self, text: str) -> str:
        return self._style(text, "38;5;221;1")

    def _line(self, fill: str = "=") -> str:
        return fill * self.width

    def _print_banner(self) -> None:
        print(self._accent(self._line("=")))
        print(self._accent(self.title.center(self.width)))
        print(self._muted(self.subtitle.center(self.width)))
        print(self._accent(self._line("=")))
        print()

    def section(self, title: str, description: str | None = None) -> None:
        print()
        print(self._accent(f"[ {title} ]"))
        if description:
            for line in textwrap.wrap(description, width=max(40, self.width - 2)):
                print(self._muted(line))
        print(self._muted(self._line("-")))
        print()

    def note(self, text: str) -> None:
        for line in textwrap.wrap(text, width=max(40, self.width - 2)):
            print(self._muted(line))

    @staticmethod
    def _questionary_default(default: str | None) -> str:
        return "" if default is None else str(default)

    def prompt(
        self,
        label: str,
        default: str | None = None,
        *,
        required: bool = False,
        hint: str | None = None,
        secret: bool = False,
    ) -> str:
        if hint:
            self.note(hint)
        if not self.interactive:
            suffix = f" [{default}]" if default not in (None, "") else ""
            while True:
                value = input(f"{label}{suffix}: ").strip()
                if value:
                    return value
                if default not in (None, ""):
                    return str(default)
                if not required:
                    return ""
        while True:
            prompt_fn = questionary.password if secret else questionary.text
            question = prompt_fn(
                label,
                default=self._questionary_default(default),
                qmark="◆",
                style=self.qstyle,
                instruction="press Enter to confirm",
            )
            value = question.ask()
            if value is None:
                raise KeyboardInterrupt
            value = value.strip()
            if value:
                return value
            if default not in (None, ""):
                return str(default)
            if not required:
                return ""
            print(self._warning("value required"))

    def boolean(self, label: str, default: bool, *, hint: str | None = None) -> bool:
        if hint:
            self.note(hint)
        if self.interactive:
            result = questionary.confirm(
                label,
                default=default,
                qmark="◆",
                style=self.qstyle,
                instruction="press y/n",
            ).ask()
            if result is None:
                raise KeyboardInterrupt
            return bool(result)
        value = self.prompt(label, "true" if default else "false", required=True)
        return value.lower() in {"1", "true", "yes", "y"}

    def summary(self, title: str, lines: list[str]) -> None:
        print()
        print(self._success(f"[ {title} ]"))
        for line in lines:
            print(f"  {line}")
        print()


def _headers(member_token: str | None) -> dict[str, str]:
    headers = {"Accept": "application/json"}
    if member_token:
        headers["Authorization"] = f"Bearer {member_token}"
    return headers


REENROLL_HINT = (
    "hub rejected the member token (expired, revoked, or the channel was pruned) — "
    "run `orbit join --no-start` on this machine to re-enroll, then retry"
)


def _raise_with_guidance(response: httpx.Response) -> None:
    if response.status_code == 401:
        raise RuntimeError(REENROLL_HINT)
    response.raise_for_status()


def _set_if_missing(args: argparse.Namespace, name: str, value) -> None:
    if getattr(args, name, None) is None and value is not None:
        setattr(args, name, value)


def _apply_config_defaults(args: argparse.Namespace, config: OrbitConfig) -> None:
    _set_if_missing(args, "hub_url", config.hub.resolved_url())
    _set_if_missing(args, "member_token", config.auth.member_token)
    _set_if_missing(args, "token_expires_at", config.auth.expires_at.isoformat() if config.auth.expires_at else None)
    _set_if_missing(args, "client_id", config.client.id)


def _set_env_if_missing(name: str, value: str | None) -> None:
    if value is None or os.getenv(name) is not None:
        return
    os.environ[name] = value


def _apply_runtime_env(config: OrbitConfig) -> None:
    _set_env_if_missing("ORBIT_HUB_HOST", config.hub.host)
    _set_env_if_missing("ORBIT_HUB_PORT", str(config.hub.port))
    _set_env_if_missing("ORBIT_HUB_DB", config.hub.db)
    _set_env_if_missing("ORBIT_OBJECT_ROOT", config.hub.object_root)
    _set_env_if_missing("ORBIT_HUB_URL", config.hub.resolved_url())
    _set_env_if_missing("ORBIT_MEMBER_TOKEN", config.auth.member_token)
    _set_env_if_missing("ORBIT_TOKEN_EXPIRES_AT", config.auth.expires_at.isoformat() if config.auth.expires_at else None)
    _set_env_if_missing("ORBIT_CLIENT_ID", config.client.id)
    _set_env_if_missing("ORBIT_WORKSPACE_ROOT", config.client.workspace_root)


def _validate_required(parser: argparse.ArgumentParser, args: argparse.Namespace, *names: str) -> None:
    missing = [name for name in names if not getattr(args, name, None)]
    if missing:
        parser.error(f"missing required configuration/arguments: {', '.join('--' + name.replace('_', '-') for name in missing)}")


def _is_terminal_command_status(value: str) -> bool:
    return value in {CommandStatus.SUCCEEDED.value, CommandStatus.FAILED.value, CommandStatus.CANCELED.value}


def _normalize_process_exit_code(value: int | None, *, default: int) -> int:
    if value is None:
        return default
    if value < 0:
        signal_code = min(abs(value), 127)
        return 128 + signal_code
    return max(0, min(value, 255))


def _command_summary_line(command_id: str, payload: dict) -> str:
    status = str(payload.get("status") or "unknown")
    parts = [f"[orbit] command {command_id} {status}"]
    if payload.get("exit_code") is not None:
        parts.append(f"exit={payload['exit_code']}")
    if payload.get("failure_code"):
        parts.append(f"reason={payload['failure_code']}")
    line = " ".join(parts)
    if payload.get("failure_code") == "unclaimed":
        line += "\n[orbit] the peer never claimed this command — it is offline or its event loop is stuck; check `orbit peers`"
    return line


def _command_result_exit_code(payload: dict) -> int:
    status = str(payload.get("status") or "")
    exit_code = payload.get("exit_code")
    failure_code = payload.get("failure_code")
    if status == CommandStatus.SUCCEEDED.value:
        return _normalize_process_exit_code(exit_code, default=0)
    if status == CommandStatus.CANCELED.value:
        if failure_code == "canceled":
            return 130
        return _normalize_process_exit_code(exit_code, default=1)
    if status == CommandStatus.FAILED.value:
        if failure_code == "timeout":
            return 124
        if failure_code == "unclaimed":
            return 125
        return _normalize_process_exit_code(exit_code, default=1)
    return 1


def _prompt_int(wizard: SetupWizard, label: str, default: int, *, hint: str | None = None) -> int:
    while True:
        value = wizard.prompt(label, str(default), required=True, hint=hint)
        try:
            return int(value)
        except ValueError:
            print(wizard._warning("enter an integer"))


def _prompt_float(wizard: SetupWizard, label: str, default: float, *, hint: str | None = None) -> float:
    while True:
        value = wizard.prompt(label, str(default), required=True, hint=hint)
        try:
            return float(value)
        except ValueError:
            print(wizard._warning("enter a number"))


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def _require_live_member_token(member_token: str | None, expires_at: str | datetime | None) -> str:
    if not member_token:
        raise RuntimeError("missing member token; run `orbit join` first")
    expiry = _parse_datetime(expires_at) if isinstance(expires_at, str) else expires_at
    if expiry is None:
        raise RuntimeError("missing token expiry; run `orbit join` first")
    if expiry <= utc_now():
        raise RuntimeError("member token expired; run `orbit join` again")
    return member_token


def _post_join_with_retry(host: str, request: JoinRequest, *, budget_sec: float = 60.0) -> dict:
    # Joining is the moment to be tolerant: retry transient network/proxy/5xx
    # errors with backoff instead of dying on the first blip. (The steady-state
    # client loop is the opposite: it gives up after repeated failures so a
    # supervisor can restart it.)
    deadline = time.monotonic() + budget_sec
    delay = 1.0
    while True:
        try:
            with httpx.Client(timeout=20) as client:
                response = client.post(f"{host}/api/join", headers=_headers(None), json=request.model_dump(mode="json"))
            if response.status_code >= 500:
                raise httpx.HTTPStatusError("server error", request=response.request, response=response)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code < 500:
                raise
            last_error = f"HTTP {exc.response.status_code}"
        except httpx.RequestError as exc:
            last_error = f"{exc.__class__.__name__}: {exc}"
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"cannot reach hub at {host} after {budget_sec:.0f}s ({last_error}) — "
                "check the network/proxy environment (a proxy 403 usually means the gateway does not whitelist this host)"
            )
        print(f"[orbit] join attempt failed ({last_error}), retrying in {delay:.0f}s", file=sys.stderr)
        time.sleep(delay)
        delay = min(8.0, delay * 2)


def _run_client_loop(config: OrbitConfig) -> int:
    from mvp_orbit.client.main import main as client_main

    _apply_runtime_env(config)
    client_main()
    return 0


def cmd_join(args: argparse.Namespace) -> int:
    config_path, config = load_config(args.config)
    host = args.host or getattr(args, "hub_url", None) or config.hub.resolved_url()
    alias = args.alias or config.client.id or getpass.getuser()
    channel = args.channel

    # Restarting with valid saved credentials must not require a fresh join
    # (which needs member approval since re-enrollment was hardened): reuse
    # the token and go straight to the client loop.
    if (
        not getattr(args, "force_rejoin", False)
        and config.auth.member_token
        and config.auth.expires_at is not None
        and config.auth.expires_at > utc_now()
        and config.client.id == alias
        and (args.host is None or config.hub.url == args.host)
    ):
        print(
            json.dumps(
                {
                    "status": "already-enrolled",
                    "alias": alias,
                    "host": config.hub.resolved_url(),
                    "token_expires_at": config.auth.expires_at.isoformat(),
                    "note": "reusing saved credentials; pass --force-rejoin to request fresh ones",
                    "started": not args.no_start,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        if args.no_start:
            return 0
        if getattr(args, "daemon", False):
            return _daemonize_and_supervise(config_path)
        return _run_client_loop(config)
    if not (args.host and args.alias and channel):
        wizard = SetupWizard(
            "ORBIT JOIN",
            "Join a shared command channel. The first client creates it; later clients need approval from an existing member.",
        )
        wizard.section("Channel", "Use the same channel name on machines that should be able to approve and control each other.")
        host = wizard.prompt("Host URL", host, required=True)
        alias = wizard.prompt("Local alias", alias, required=True)
        channel = wizard.prompt("Channel name", channel, required=True)
    request = JoinRequest(alias=alias, channel=channel)
    payload = _post_join_with_retry(host, request)

    if payload["status"] == JoinRequestStatus.PENDING.value:
        request_id = payload["request_id"]
        claim_secret = payload.get("claim_secret")
        print(json.dumps({"status": "pending", "request_id": request_id, "alias": alias, "channel_id": payload["channel_id"], "claim_secret": claim_secret}, ensure_ascii=False, indent=2))
        if args.no_wait:
            return 0
        deadline = time.monotonic() + args.wait_sec
        while time.monotonic() < deadline:
            time.sleep(2.0)
            try:
                with httpx.Client(timeout=20) as client:
                    response = client.get(
                        f"{host}/api/join-requests/{request_id}",
                        headers=_headers(None),
                        params={"secret": claim_secret} if claim_secret else None,
                    )
                    response.raise_for_status()
                    payload = response.json()
            except httpx.RequestError as exc:
                print(f"[orbit] transient error while waiting for approval ({exc.__class__.__name__}), retrying", file=sys.stderr)
                continue
            if payload["status"] == JoinRequestStatus.APPROVED.value:
                break
            if payload["status"] == JoinRequestStatus.REJECTED.value:
                print(json.dumps(payload, ensure_ascii=False), file=sys.stderr)
                return 1
        else:
            print(f"join request still pending: {request_id}", file=sys.stderr)
            return 124

    if not payload.get("member_token"):
        print(
            "[orbit] join approved but the credential was already issued to an earlier poll of this request — run `orbit join` again",
            file=sys.stderr,
        )
        return 1

    config.hub.url = host
    config.client.id = alias
    config.auth.member_token = payload["member_token"]
    config.auth.expires_at = _parse_datetime(payload["expires_at"])
    if not config.client.workspace_root:
        # Pin the workspace at join time; otherwise put/get land wherever the
        # client process happened to be started from on any given day.
        config.client.workspace_root = str(Path.cwd())
    saved_path = save_config(config, config_path)
    print(
        json.dumps(
            {
                "status": payload["status"],
                "config_path": str(saved_path),
                "alias": alias,
                "host": host,
                "channel_id": payload.get("channel_id"),
                "token_expires_at": payload["expires_at"],
                "workspace_root": config.client.workspace_root,
                "started": not args.no_start,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    if args.no_start:
        return 0
    if getattr(args, "daemon", False):
        return _daemonize_and_supervise(config_path)
    return _run_client_loop(config)


def cmd_join_requests(args: argparse.Namespace) -> int:
    member_token = _require_live_member_token(args.member_token, args.token_expires_at)
    params = {"status": args.status} if args.status else None
    with httpx.Client(timeout=20) as client:
        response = client.get(f"{args.hub_url}/api/join-requests", headers=_headers(member_token), params=params)
        _raise_with_guidance(response)
        print(json.dumps(response.json(), ensure_ascii=False, indent=2))
    return 0


def cmd_approve_join(args: argparse.Namespace) -> int:
    member_token = _require_live_member_token(args.member_token, args.token_expires_at)
    with httpx.Client(timeout=20) as client:
        response = client.post(f"{args.hub_url}/api/join-requests/{args.request_id}/approve", headers=_headers(member_token))
        _raise_with_guidance(response)
        print(json.dumps(response.json(), ensure_ascii=False, indent=2))
    return 0


def cmd_reject_join(args: argparse.Namespace) -> int:
    member_token = _require_live_member_token(args.member_token, args.token_expires_at)
    with httpx.Client(timeout=20) as client:
        response = client.post(f"{args.hub_url}/api/join-requests/{args.request_id}/reject", headers=_headers(member_token))
        _raise_with_guidance(response)
        print(json.dumps(response.json(), ensure_ascii=False, indent=2))
    return 0


def cmd_peers(args: argparse.Namespace) -> int:
    member_token = _require_live_member_token(args.member_token, args.token_expires_at)
    with httpx.Client(timeout=20) as client:
        response = client.get(f"{args.hub_url}/api/peers", headers=_headers(member_token))
        _raise_with_guidance(response)
        print(json.dumps(response.json(), ensure_ascii=False, indent=2))
    return 0


def _extract_trailing_exec_options(args: argparse.Namespace) -> None:
    # argparse.REMAINDER swallows everything after the peer name, including
    # options ("orbit exec peer --shell '…'" used to send the literal string
    # "--shell" to the peer as argv[0] and hang). Recognize our own options at
    # the head of the remainder so both placements work.
    argv = list(args.command_argv or [])
    valued = {"--timeout-sec": "timeout_sec", "--claim-timeout": "claim_timeout", "--working-dir": "working_dir"}
    while argv and argv[0] != "--" and argv[0].startswith("--"):
        option, _, inline_value = argv[0].partition("=")
        if option == "--shell":
            args.shell = True
            argv.pop(0)
            continue
        if option in valued:
            if inline_value:
                value = inline_value
                argv.pop(0)
            elif len(argv) >= 2:
                value = argv[1]
                del argv[:2]
            else:
                raise SystemExit(f"orbit exec: option {option} requires a value")
            if option == "--working-dir":
                args.working_dir = value
            else:
                try:
                    setattr(args, valued[option], int(value))
                except ValueError:
                    raise SystemExit(f"orbit exec: option {option} expects an integer, got {value!r}") from None
            continue
        raise SystemExit(
            f"orbit exec: unknown option {option!r} after the peer name — "
            "place orbit options before the peer, or use `--` to pass literal arguments to the remote command"
        )
    args.command_argv = argv


def cmd_exec_peer(args: argparse.Namespace) -> int:
    if getattr(args, "to", None):
        if getattr(args, "target", None):
            args.command_argv = [args.target] + list(args.command_argv or [])
        args.client_id = args.to
    else:
        args.client_id = args.target
    _extract_trailing_exec_options(args)
    args.working_dir = args.working_dir or "."
    args.env_file = None
    args.detach = False
    return cmd_command_exec(args)


def cmd_shell_peer(args: argparse.Namespace) -> int:
    args.client_id = args.target
    args.detach = False
    return cmd_shell_start(args)


def cmd_put(args: argparse.Namespace) -> int:
    args.to = args.target
    return cmd_file_push(args)


def cmd_get(args: argparse.Namespace) -> int:
    args.source = args.target
    return cmd_file_pull(args)


def cmd_file_push(args: argparse.Namespace) -> int:
    member_token = _require_live_member_token(args.member_token, args.token_expires_at)
    local_path = Path(args.local_path)
    data = local_path.read_bytes()
    if len(data) > args.max_bytes:
        raise RuntimeError(f"local file exceeds max bytes: {len(data)} > {args.max_bytes}")
    request = FilePushRequest(
        client_id=args.to,
        remote_path=args.remote_path,
        data_b64=base64.b64encode(data).decode("ascii"),
        max_bytes=args.max_bytes,
    )
    with httpx.Client(timeout=60) as client:
        response = client.post(
            f"{args.hub_url}/api/files/push",
            headers=_headers(member_token),
            json=request.model_dump(mode="json"),
        )
        _raise_with_guidance(response)
        transfer_id = response.json()["transfer_id"]
    result = _follow_file_transfer(args.hub_url, member_token, transfer_id)
    if result.get("status") != FileTransferStatus.SUCCEEDED.value:
        print(json.dumps(result, ensure_ascii=False), file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False))
    return 0


def cmd_file_pull(args: argparse.Namespace) -> int:
    member_token = _require_live_member_token(args.member_token, args.token_expires_at)
    request = FilePullRequest(client_id=args.source, remote_path=args.remote_path, max_bytes=args.max_bytes)
    with httpx.Client(timeout=20) as client:
        response = client.post(
            f"{args.hub_url}/api/files/pull",
            headers=_headers(member_token),
            json=request.model_dump(mode="json"),
        )
        _raise_with_guidance(response)
        transfer_id = response.json()["transfer_id"]
    result = _follow_file_transfer(args.hub_url, member_token, transfer_id)
    if result.get("status") != FileTransferStatus.SUCCEEDED.value:
        print(json.dumps(result, ensure_ascii=False), file=sys.stderr)
        return 1
    data_b64 = result.get("data_b64")
    if not data_b64:
        raise RuntimeError("file transfer succeeded without payload")
    data = base64.b64decode(data_b64.encode("ascii"), validate=True)
    if len(data) > args.max_bytes:
        raise RuntimeError(f"remote file exceeds max bytes: {len(data)} > {args.max_bytes}")
    local_path = Path(args.local_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_bytes(data)
    print(json.dumps({k: v for k, v in result.items() if k != "data_b64"}, ensure_ascii=False))
    return 0

def _command_create_request(args: argparse.Namespace) -> CommandCreateRequest:
    argv = list(args.command_argv or [])
    if argv and argv[0] == "--":
        argv = argv[1:]
    if getattr(args, "shell", False):
        argv = _shell_wrapped_argv(" ".join(argv))
    elif len(argv) == 1 and _looks_like_shell_command(argv[0]):
        argv = _shell_wrapped_argv(argv[0])
    return CommandCreateRequest(
        client_id=args.client_id,
        argv=argv,
        env_patch=_load_json(args.env_file) if args.env_file else {},
        timeout_sec=args.timeout_sec,
        working_dir=args.working_dir,
        claim_timeout_sec=getattr(args, "claim_timeout", None),
    )


def _looks_like_shell_command(value: str) -> bool:
    return " " in value or bool(_SHELL_META_PATTERN.search(value))


def _shell_wrapped_argv(command: str) -> list[str]:
    return ["/bin/sh", "-lc", command]


def cmd_command_exec(args: argparse.Namespace) -> int:
    member_token = _require_live_member_token(args.member_token, args.token_expires_at)
    request = _command_create_request(args)
    with httpx.Client(timeout=60) as client:
        response = client.post(
            f"{args.hub_url}/api/commands",
            headers=_headers(member_token),
            # exclude_none keeps requests compatible with hubs that predate
            # optional fields like claim_timeout_sec (extra="forbid" models)
            json=request.model_dump(mode="json", exclude_none=True),
        )
        _raise_with_guidance(response)
        payload = response.json()
        command_id = payload["command_id"]
        if args.detach:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return 0
    final_payload = _follow_command_output(args.hub_url, member_token, command_id)
    print(_command_summary_line(command_id, final_payload), file=sys.stderr, flush=True)
    return _command_result_exit_code(final_payload)


def _iter_sse_events(response: httpx.Response) -> list[dict]:
    block: list[str] = []
    for line in response.iter_lines():
        if line == "":
            if not block:
                continue
            event_type = "message"
            data_lines: list[str] = []
            event_id = None
            for item in block:
                if not item or item.startswith(":"):
                    continue
                if item.startswith("event:"):
                    event_type = item.partition(":")[2].strip()
                elif item.startswith("id:"):
                    event_id = item.partition(":")[2].strip()
                elif item.startswith("data:"):
                    data_lines.append(item.partition(":")[2].lstrip())
            block = []
            if not data_lines:
                continue
            raw_data = "\n".join(data_lines)
            try:
                payload = json.loads(raw_data)
            except json.JSONDecodeError:
                payload = {"data": raw_data}
            yield {"id": event_id, "event": event_type, "payload": payload}
            continue
        block.append(line)


def _follow_stream(hub_url: str, member_token: str, path: str, on_event) -> dict | None:
    """Consume an SSE stream until on_event returns a terminal payload.

    The hub emits a keepalive at least every 5s, so a finite read timeout only
    fires when the connection is actually dead; then we reconnect with
    Last-Event-ID (events are persisted server-side, nothing is lost) and give
    up loudly after repeated failures instead of hanging forever.
    Returns None when the server closes the stream without a terminal event.
    """
    timeout = httpx.Timeout(connect=5.0, read=30.0, write=5.0, pool=5.0)
    last_event_id: str | None = None
    failures = 0
    with httpx.Client(timeout=timeout) as client:
        while True:
            headers = _headers(member_token) | {"Accept": "text/event-stream"}
            if last_event_id:
                headers["Last-Event-ID"] = last_event_id
            try:
                with client.stream("GET", f"{hub_url}{path}", headers=headers) as response:
                    if response.status_code == 401:
                        raise RuntimeError(REENROLL_HINT)
                    response.raise_for_status()
                    failures = 0
                    for event in _iter_sse_events(response):
                        if event.get("id"):
                            last_event_id = event["id"]
                        result = on_event(event)
                        if result is not None:
                            return result
                return None
            except httpx.RequestError as exc:
                failures += 1
                if failures >= 6:
                    raise RuntimeError(
                        f"lost connection to hub while streaming {path} ({exc.__class__.__name__}: {exc})"
                    ) from exc
                print(f"[orbit] stream interrupted ({exc.__class__.__name__}), reconnecting", file=sys.stderr, flush=True)
                time.sleep(min(10.0, 2.0 ** failures))


def _fetch_json(hub_url: str, member_token: str, path: str) -> dict:
    with httpx.Client(timeout=20) as client:
        response = client.get(f"{hub_url}{path}", headers=_headers(member_token))
        _raise_with_guidance(response)
        return response.json()


def _follow_file_transfer(hub_url: str, member_token: str, transfer_id: str) -> dict:
    def on_event(event: dict) -> dict | None:
        if event["event"] == "file.result":
            return event["payload"]
        return None

    result = _follow_stream(hub_url, member_token, f"/api/files/{transfer_id}/stream", on_event)
    if result is not None:
        return result
    record = _fetch_json(hub_url, member_token, f"/api/files/{transfer_id}")
    if record.get("status") in {FileTransferStatus.SUCCEEDED.value, FileTransferStatus.FAILED.value}:
        return record
    raise RuntimeError(f"file transfer stream ended before terminal event for {transfer_id}")


def _follow_command_output(hub_url: str, member_token: str, command_id: str) -> dict:
    def on_event(event: dict) -> dict | None:
        payload = event["payload"]
        if event["event"] == "command.stdout":
            print(payload.get("data", ""), end="", file=sys.stdout, flush=True)
        elif event["event"] == "command.stderr":
            print(payload.get("data", ""), end="", file=sys.stderr, flush=True)
        elif event["event"] == "command.exit":
            return payload
        return None

    result = _follow_stream(hub_url, member_token, f"/api/commands/{command_id}/stream", on_event)
    if result is not None:
        return result
    record = _fetch_json(hub_url, member_token, f"/api/commands/{command_id}")
    if _is_terminal_command_status(str(record.get("status"))):
        return {
            "command_id": command_id,
            "status": record.get("status"),
            "exit_code": record.get("exit_code"),
            "failure_code": record.get("failure_code"),
        }
    raise RuntimeError(f"command stream ended before terminal event for {command_id}")


def cmd_shell_start(args: argparse.Namespace) -> int:
    member_token = _require_live_member_token(args.member_token, args.token_expires_at)
    request = ShellSessionCreateRequest(client_id=args.client_id)
    with httpx.Client(timeout=20) as client:
        response = client.post(
            f"{args.hub_url}/api/shells",
            headers=_headers(member_token),
            json=request.model_dump(mode="json"),
        )
        _raise_with_guidance(response)
        payload = response.json()
    if args.detach or not sys.stdin.isatty():
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    print(f"[orbit] shell session {payload['session_id']}", file=sys.stderr, flush=True)
    _attach_shell(args.hub_url, member_token, payload["session_id"])
    return 0


def _post_shell_resize(hub_url: str, member_token: str, session_id: str) -> None:
    size = shutil.get_terminal_size((80, 24))
    with httpx.Client(timeout=10.0) as client:
        response = client.post(
            f"{hub_url}/api/shells/{session_id}/resize",
            headers=_headers(member_token),
            json=ShellResizeRequest(rows=size.lines, cols=size.columns).model_dump(mode="json"),
        )
        _raise_with_guidance(response)


def _attach_shell(hub_url: str, member_token: str, session_id: str) -> None:
    stop = threading.Event()
    stream_error: list[BaseException] = []

    def consume_events() -> None:
        def on_event(event: dict) -> dict | None:
            payload = event["payload"]
            if event["event"] == "shell.stdout":
                print(payload.get("data", ""), end="", file=sys.stdout, flush=True)
            elif event["event"] == "shell.stderr":
                print(payload.get("data", ""), end="", file=sys.stderr, flush=True)
            elif event["event"] in {"shell.closed", "shell.exit"}:
                return payload
            return None

        try:
            _follow_stream(hub_url, member_token, f"/api/shells/{session_id}/stream", on_event)
        except BaseException as exc:
            stream_error.append(exc)
        finally:
            stop.set()

    thread = threading.Thread(target=consume_events, daemon=True)
    thread.start()
    if not sys.stdin.isatty():
        thread.join()
        if stream_error:
            raise stream_error[0]
        return

    fd = sys.stdin.fileno()
    old_attrs = termios.tcgetattr(fd)
    resize_pending = threading.Event()
    previous_handler = signal.getsignal(signal.SIGWINCH)

    def on_winch(signum, frame) -> None:
        resize_pending.set()

    try:
        signal.signal(signal.SIGWINCH, on_winch)
        tty.setraw(fd)
        resize_pending.set()
        while not stop.is_set():
            if resize_pending.is_set():
                resize_pending.clear()
                _post_shell_resize(hub_url, member_token, session_id)
            ready, _, _ = select.select([fd], [], [], 0.1)
            if not ready:
                continue
            data = os.read(fd, 1024)
            if not data:
                break
            with httpx.Client(timeout=10.0) as client:
                response = client.post(
                    f"{hub_url}/api/shells/{session_id}/input",
                    headers=_headers(member_token),
                    json={"data": data.decode("utf-8", errors="replace")},
                )
                _raise_with_guidance(response)
    except KeyboardInterrupt:
        pass
    finally:
        signal.signal(signal.SIGWINCH, previous_handler)
        termios.tcsetattr(fd, termios.TCSADRAIN, old_attrs)
        stop.set()
        thread.join(timeout=2)
    if stream_error:
        raise stream_error[0]


def _post_json(hub_url: str, member_token: str, path: str, payload: dict | None = None) -> dict:
    with httpx.Client(timeout=20) as client:
        response = client.post(f"{hub_url}{path}", headers=_headers(member_token), json=payload)
        if response.status_code in {403, 404, 409}:
            try:
                detail = response.json().get("detail")
            except Exception:
                detail = response.text
            raise RuntimeError(str(detail))
        _raise_with_guidance(response)
        return response.json()


def _confirm_or_yes(args: argparse.Namespace, question: str) -> bool:
    if getattr(args, "yes", False):
        return True
    if not sys.stdin.isatty():
        raise RuntimeError("refusing a destructive action without a terminal — pass --yes to confirm")
    answer = input(f"{question} [y/N]: ").strip().lower()
    return answer in {"y", "yes"}


def cmd_members(args: argparse.Namespace) -> int:
    member_token = _require_live_member_token(args.member_token, args.token_expires_at)
    members = _fetch_json(args.hub_url, member_token, "/api/members")
    print(json.dumps(members, ensure_ascii=False, indent=2))
    return 0


def cmd_leave(args: argparse.Namespace) -> int:
    member_token = _require_live_member_token(args.member_token, args.token_expires_at)
    if not _confirm_or_yes(args, "Leave this channel? Your credentials on this machine stop working immediately"):
        return 1
    result = _post_json(args.hub_url, member_token, "/api/members/leave")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    # The token is revoked server-side; drop it locally too.
    config_path, config = load_config(args.config)
    config.auth.member_token = None
    config.auth.expires_at = None
    save_config(config, config_path)
    return 0


def cmd_remove_member(args: argparse.Namespace) -> int:
    member_token = _require_live_member_token(args.member_token, args.token_expires_at)
    if not _confirm_or_yes(args, f"Remove {args.target!r} from the channel and revoke its credentials"):
        return 1
    result = _post_json(args.hub_url, member_token, f"/api/members/{args.target}/remove")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def cmd_transfer_admin(args: argparse.Namespace) -> int:
    member_token = _require_live_member_token(args.member_token, args.token_expires_at)
    promoted = _post_json(args.hub_url, member_token, f"/api/members/{args.target}/role", {"role": "admin"})
    print(json.dumps(promoted, ensure_ascii=False, indent=2))
    if not args.keep_admin:
        _, config = load_config(args.config)
        own_alias = config.client.id
        if own_alias and own_alias != args.target:
            demoted = _post_json(args.hub_url, member_token, f"/api/members/{own_alias}/role", {"role": "member"})
            print(json.dumps(demoted, ensure_ascii=False, indent=2))
    return 0


def cmd_hub_serve(args: argparse.Namespace) -> int:
    from mvp_orbit.hub.app import main as hub_main

    _apply_runtime_env(args._orbit_config)
    hub_main()
    return 0


def _force_runtime_env(config: OrbitConfig) -> None:
    values = {
        "ORBIT_HUB_URL": config.hub.resolved_url(),
        "ORBIT_MEMBER_TOKEN": config.auth.member_token,
        "ORBIT_TOKEN_EXPIRES_AT": config.auth.expires_at.isoformat() if config.auth.expires_at else None,
        "ORBIT_CLIENT_ID": config.client.id,
        "ORBIT_WORKSPACE_ROOT": config.client.workspace_root,
    }
    for name, value in values.items():
        if value is not None:
            os.environ[name] = str(value)


def _daemon_paths(config_path: str):
    from mvp_orbit.client.service import state_dir

    _, config = load_config(config_path)
    alias = config.client.id or "client"
    root = state_dir()
    if root is None:
        raise RuntimeError("cannot determine a state directory for daemon logs (set ORBIT_STATE_DIR)")
    root.mkdir(parents=True, exist_ok=True)
    return root / f"daemon-{alias}.log", root / f"daemon-{alias}.pid"


def _daemonize_and_supervise(config_path: str) -> int:
    # Detach via fork+exec (subprocess), NOT a bare os.fork(): forking a Python
    # process that has touched CoreFoundation (httpx's system-proxy lookup
    # does) segfaults the child on macOS — "crashed on child side of fork".
    log_path, pid_path = _daemon_paths(config_path)
    if pid_path.exists():
        content = pid_path.read_text().strip()
        try:
            existing = int(content)
        except ValueError:
            # A "starting" placeholder: either a concurrent invocation mid-spawn
            # (fresh — refuse, do NOT unlink its reservation) or debris from a
            # crash before the pid was written (old — clean up).
            if time.time() - pid_path.stat().st_mtime < 60.0:
                raise RuntimeError(f"a daemon is already starting (pidfile {pid_path}); retry shortly") from None
            pid_path.unlink(missing_ok=True)
        else:
            try:
                os.kill(existing, 0)
            except ProcessLookupError:
                pid_path.unlink(missing_ok=True)
            except PermissionError:
                raise RuntimeError(f"daemon already running (pid {existing}, owned by another user)") from None
            else:
                raise RuntimeError(f"daemon already running (pid {existing}); stop it with `kill {existing}` first")

    try:
        # Atomically reserve the pidfile so two concurrent `join --daemon`
        # invocations cannot both spawn a supervisor.
        reservation = os.open(pid_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.write(reservation, b"starting")
        os.close(reservation)
    except FileExistsError:
        raise RuntimeError(f"daemon already starting or running (pidfile {pid_path} exists)") from None
    log_fd = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        process = subprocess.Popen(
            [sys.executable, "-m", "mvp_orbit.cli.main", "--config", str(config_path), "daemon-supervise"],
            stdin=subprocess.DEVNULL,
            stdout=log_fd,
            stderr=log_fd,
            start_new_session=True,
            close_fds=True,
        )
    except BaseException:
        pid_path.unlink(missing_ok=True)
        raise
    finally:
        os.close(log_fd)
    print(json.dumps({"daemon": True, "pid": process.pid, "log": str(log_path), "pidfile": str(pid_path)}, ensure_ascii=False, indent=2))
    return 0


def cmd_daemon_supervise(args: argparse.Namespace) -> int:
    # The detached supervisor process: restart the client loop with backoff
    # until the token expires or someone stops us.
    config_path = args.config
    _, pid_path = _daemon_paths(config_path)
    pid_path.write_text(str(os.getpid()), encoding="utf-8")

    def on_term(signum, frame):
        print(f"[orbit-daemon] received signal {signum}, exiting", flush=True)
        pid_path.unlink(missing_ok=True)
        os._exit(0)

    signal.signal(signal.SIGTERM, on_term)
    delay = 10.0
    try:
        while True:
            _, config = load_config(config_path)
            if config.auth.expires_at is None or config.auth.expires_at <= utc_now():
                print("[orbit-daemon] member token expired — run `orbit join` again, then restart the daemon", flush=True)
                break
            _force_runtime_env(config)
            started = time.monotonic()
            try:
                _run_client_loop(config)
                code: int | str | None = 0
            except SystemExit as exc:
                code = exc.code
            except Exception as exc:  # noqa: BLE001 — the supervisor must survive anything
                print(f"[orbit-daemon] client crashed: {exc.__class__.__name__}: {exc}", flush=True)
                code = 1
            # A loop that ran for a while earns a fresh backoff.
            delay = 10.0 if time.monotonic() - started > 120 else min(60.0, delay * 2)
            print(f"[orbit-daemon] client exited (code={code}), restarting in {delay:.0f}s", flush=True)
            time.sleep(delay)
    finally:
        pid_path.unlink(missing_ok=True)
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    from mvp_orbit.client.service import status_file_path

    _, config = load_config(args.config)
    client_id = getattr(args, "client_id", None) or config.client.id
    if not client_id:
        raise RuntimeError("no client id configured — run `orbit join` first")
    path = status_file_path(client_id)
    if path is None or not path.exists():
        print(f"[orbit] no status file for {client_id!r} — the client loop has never run on this machine (or ORBIT_STATE_DIR differs)")
        return 1
    payload = json.loads(path.read_text(encoding="utf-8"))
    pid = payload.get("pid")
    alive = False
    if isinstance(pid, int):
        try:
            os.kill(pid, 0)
            alive = True
        except (ProcessLookupError, PermissionError):
            alive = False
    age = time.time() - float(payload.get("updated_at") or 0)
    connected = bool(payload.get("stream_connected"))
    stream_line = "connected" if connected else "DISCONNECTED"
    if not alive:
        stream_line = f"unknown (last written while running: {stream_line})"
    lines = [
        f"client:    {payload.get('client_id')}",
        f"hub:       {payload.get('hub_url')}",
        f"process:   {'running' if alive else 'NOT RUNNING'} (pid {pid}, status written {age:.0f}s ago)",
        f"stream:    {stream_line}",
        f"workspace: {payload.get('workspace')}",
    ]
    if payload.get("last_stream_error"):
        lines.append(f"last error: {payload['last_stream_error']}")
    if config.auth.expires_at is not None:
        lines.append(f"token:     expires {config.auth.expires_at.isoformat()}")
    print("\n".join(lines))
    if not alive:
        print("\n[orbit] the client process is not running — start it with `orbit join --daemon` (or your supervisor)")
        return 1
    if not connected or age > 60:
        print("\n[orbit] the process is alive but the event stream is not healthy — check the last error above; if it persists, restart the client")
        return 2
    return 0


def cmd_doctor(args: argparse.Namespace) -> int:
    member_token = _require_live_member_token(args.member_token, args.token_expires_at)
    target = args.target
    print(f"[orbit doctor] token: valid (expires {args.token_expires_at})")

    peers = _fetch_json(args.hub_url, member_token, "/api/peers")
    record = next((item for item in peers if item.get("client_id") == target), None)
    if record is None:
        print(f"[orbit doctor] peer {target!r}: UNKNOWN to the hub — it never connected in this channel (check the alias, or start its client loop)")
        return 2
    last_seen = _parse_datetime(record.get("last_seen_at"))
    age = (utc_now() - last_seen).total_seconds() if last_seen else None
    stream_connected = record.get("stream_connected")
    print(f"[orbit doctor] peer {target!r}: last_seen {'never' if age is None else f'{age:.0f}s ago'}, stream_connected={stream_connected}")

    print(f"[orbit doctor] probing with a {args.claim_timeout}s claim-timeout echo …")
    request = CommandCreateRequest(client_id=target, argv=["echo", "orbit-doctor-probe"], timeout_sec=60, claim_timeout_sec=args.claim_timeout)
    with httpx.Client(timeout=20) as client:
        response = client.post(f"{args.hub_url}/api/commands", headers=_headers(member_token), json=request.model_dump(mode="json", exclude_none=True))
        _raise_with_guidance(response)
        command_id = response.json()["command_id"]
    outcome: dict = {}

    def on_event(event: dict) -> dict | None:
        return event["payload"] if event["event"] == "command.exit" else None

    outcome = _follow_stream(args.hub_url, member_token, f"/api/commands/{command_id}/stream", on_event) or {}

    status = outcome.get("status")
    failure = outcome.get("failure_code")
    heartbeat_fresh = age is not None and age < 45
    if status == "succeeded":
        print(f"[orbit doctor] VERDICT: healthy — {target} claimed and ran the probe")
        return 0
    if failure == "unclaimed" and not heartbeat_fresh:
        print(f"[orbit doctor] VERDICT: process dead — no heartbeat and no claim; restart the client loop on {target}")
    elif failure == "unclaimed" and stream_connected is False:
        print(f"[orbit doctor] VERDICT: deaf client — heartbeats arrive but the event stream is down on {target}; restart its client loop")
    elif failure == "unclaimed":
        print(f"[orbit doctor] VERDICT: peer looks alive but did not claim in {args.claim_timeout}s — its event loop may be stuck; restart its client loop")
    else:
        print(f"[orbit doctor] VERDICT: probe finished with status={status} failure={failure} — see output above")
    return 1


def _load_json(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="orbit", description="mvp-orbit CLI")
    parser.add_argument("--config", default=os.getenv("ORBIT_CONFIG"), help="path to config.toml")
    sub = parser.add_subparsers(
        dest="command",
        required=True,
        metavar="{host,join,join-requests,approve,reject,peers,exec,sh,put,get,status,doctor,members,leave,remove,transfer-admin}",
    )

    host = sub.add_parser("host", help="start the control host")
    host.set_defaults(func=cmd_hub_serve)

    join = sub.add_parser("join", help="join and start this client")
    join.add_argument("--host", "--hub-url", dest="host", default=None)
    join.add_argument("--alias", default=None)
    join.add_argument("--channel", default=None)
    join.add_argument("--wait-sec", type=int, default=600)
    join.add_argument("--no-wait", action="store_true")
    join.add_argument("--no-start", action="store_true", help="join and save config without starting the client loop")
    join.add_argument("--daemon", action="store_true", help="run the client loop as a supervised background daemon (auto-restart, log under ~/.local/state/mvp-orbit)")
    join.add_argument("--force-rejoin", action="store_true", help="request fresh credentials even when valid saved ones exist (needs member approval)")
    join.set_defaults(func=cmd_join)

    status = sub.add_parser("status", help="show this machine's client state (local, no token needed)")
    status.add_argument("--client-id", default=None)
    status.set_defaults(func=cmd_status)

    doctor = sub.add_parser("doctor", help="diagnose a peer: orbit doctor <peer>")
    doctor.add_argument("--hub-url", default=None)
    doctor.add_argument("--member-token", default=os.getenv("ORBIT_MEMBER_TOKEN"))
    doctor.add_argument("--token-expires-at", default=os.getenv("ORBIT_TOKEN_EXPIRES_AT"))
    doctor.add_argument("--claim-timeout", type=int, default=10)
    doctor.add_argument("target")
    doctor.set_defaults(func=cmd_doctor)

    supervise = sub.add_parser("daemon-supervise")  # internal: exec'd by `orbit join --daemon`
    supervise.set_defaults(func=cmd_daemon_supervise)

    def _token_args(p) -> None:
        p.add_argument("--hub-url", default=None)
        p.add_argument("--member-token", default=os.getenv("ORBIT_MEMBER_TOKEN"))
        p.add_argument("--token-expires-at", default=os.getenv("ORBIT_TOKEN_EXPIRES_AT"))

    members = sub.add_parser("members", help="list channel members with roles and liveness")
    _token_args(members)
    members.set_defaults(func=cmd_members)

    leave = sub.add_parser("leave", help="leave the channel and revoke this machine's credentials")
    _token_args(leave)
    leave.add_argument("--yes", action="store_true", help="skip the confirmation prompt")
    leave.set_defaults(func=cmd_leave)

    remove = sub.add_parser("remove", help="admin: remove a member and revoke its credentials: orbit remove <alias>")
    _token_args(remove)
    remove.add_argument("--yes", action="store_true", help="skip the confirmation prompt")
    remove.add_argument("target")
    remove.set_defaults(func=cmd_remove_member)

    transfer = sub.add_parser("transfer-admin", help="admin: promote <alias> to admin and step down: orbit transfer-admin <alias>")
    _token_args(transfer)
    transfer.add_argument("--keep-admin", action="store_true", help="promote the target but stay admin yourself")
    transfer.add_argument("target")
    transfer.set_defaults(func=cmd_transfer_admin)

    join_requests = sub.add_parser("join-requests", help="list pending join requests")
    join_requests.add_argument("--hub-url", default=None)
    join_requests.add_argument("--member-token", default=os.getenv("ORBIT_MEMBER_TOKEN"))
    join_requests.add_argument("--token-expires-at", default=os.getenv("ORBIT_TOKEN_EXPIRES_AT"))
    join_requests.add_argument("--status", choices=[item.value for item in JoinRequestStatus], default=JoinRequestStatus.PENDING.value)
    join_requests.set_defaults(func=cmd_join_requests)

    approve = sub.add_parser("approve", help="approve a join request")
    approve.add_argument("request_id")
    approve.add_argument("--hub-url", default=None)
    approve.add_argument("--member-token", default=os.getenv("ORBIT_MEMBER_TOKEN"))
    approve.add_argument("--token-expires-at", default=os.getenv("ORBIT_TOKEN_EXPIRES_AT"))
    approve.set_defaults(func=cmd_approve_join)

    reject = sub.add_parser("reject", help="reject a join request")
    reject.add_argument("request_id")
    reject.add_argument("--hub-url", default=None)
    reject.add_argument("--member-token", default=os.getenv("ORBIT_MEMBER_TOKEN"))
    reject.add_argument("--token-expires-at", default=os.getenv("ORBIT_TOKEN_EXPIRES_AT"))
    reject.set_defaults(func=cmd_reject_join)

    peers = sub.add_parser("peers", help="list clients in the current channel")
    peers.add_argument("--hub-url", default=None)
    peers.add_argument("--member-token", default=os.getenv("ORBIT_MEMBER_TOKEN"))
    peers.add_argument("--token-expires-at", default=os.getenv("ORBIT_TOKEN_EXPIRES_AT"))
    peers.set_defaults(func=cmd_peers)

    exec_cmd = sub.add_parser("exec", help="send one command: orbit exec <peer> -- <command>")
    exec_cmd.add_argument("--hub-url", default=None)
    exec_cmd.add_argument("--member-token", default=os.getenv("ORBIT_MEMBER_TOKEN"))
    exec_cmd.add_argument("--token-expires-at", default=os.getenv("ORBIT_TOKEN_EXPIRES_AT"))
    exec_cmd.add_argument("--working-dir", default=".")
    exec_cmd.add_argument("--timeout-sec", type=int, default=3600)
    exec_cmd.add_argument("--claim-timeout", type=int, default=None, help="fail fast if no peer claims the command within N seconds (default: hub setting, 30s)")
    exec_cmd.add_argument("--shell", action="store_true", help="run the trailing command through /bin/sh -lc on the peer")
    exec_cmd.add_argument("target")
    exec_cmd.add_argument("command_argv", nargs=argparse.REMAINDER)
    exec_cmd.set_defaults(func=cmd_exec_peer)

    sh = sub.add_parser("sh", help="open an interactive shell: orbit sh <peer>")
    sh.add_argument("--hub-url", default=None)
    sh.add_argument("--member-token", default=os.getenv("ORBIT_MEMBER_TOKEN"))
    sh.add_argument("--token-expires-at", default=os.getenv("ORBIT_TOKEN_EXPIRES_AT"))
    sh.add_argument("target")
    sh.set_defaults(func=cmd_shell_peer)

    put = sub.add_parser("put", help="send a file: orbit put <peer> <local> <remote>")
    put.add_argument("--hub-url", default=None)
    put.add_argument("--member-token", default=os.getenv("ORBIT_MEMBER_TOKEN"))
    put.add_argument("--token-expires-at", default=os.getenv("ORBIT_TOKEN_EXPIRES_AT"))
    put.add_argument("--max-bytes", type=int, default=1024 * 1024)
    put.add_argument("target")
    put.add_argument("local_path")
    put.add_argument("remote_path")
    put.set_defaults(func=cmd_put)

    get = sub.add_parser("get", help="fetch a file: orbit get <peer> <remote> <local>")
    get.add_argument("--hub-url", default=None)
    get.add_argument("--member-token", default=os.getenv("ORBIT_MEMBER_TOKEN"))
    get.add_argument("--token-expires-at", default=os.getenv("ORBIT_TOKEN_EXPIRES_AT"))
    get.add_argument("--max-bytes", type=int, default=1024 * 1024)
    get.add_argument("target")
    get.add_argument("remote_path")
    get.add_argument("local_path")
    get.set_defaults(func=cmd_get)

    return parser

def prepare_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> argparse.Namespace:
    config_path, config = load_config(args.config)
    args.config = str(config_path)
    args._orbit_config = config
    _apply_config_defaults(args, config)

    if args.command == "join":
        if getattr(args, "host", None) is None:
            args.host = config.hub.resolved_url()
    if args.command in {"join-requests", "approve", "reject", "peers", "exec", "sh", "put", "get", "doctor", "members", "leave", "remove", "transfer-admin"}:
        _validate_required(parser, args, "hub_url", "member_token", "token_expires_at")
    if args.command == "exec":
        argv = list(args.command_argv or [])
        if argv and argv[0] == "--":
            argv = argv[1:]
        if not argv:
            parser.error("exec requires a trailing command, for example: orbit exec client-b -- python3 -V")
    return args

def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = prepare_args(parser, parser.parse_args(argv))
    try:
        result = args.func(args)
    except RuntimeError as exc:
        print(f"[orbit] error: {exc}", file=sys.stderr)
        return 1
    except httpx.RequestError as exc:
        print(
            f"[orbit] error: cannot reach the hub ({exc.__class__.__name__}: {exc}) — is it running, and is this machine's network/proxy environment sane?",
            file=sys.stderr,
        )
        return 1
    except KeyboardInterrupt:
        return 130
    return int(result or 0)


if __name__ == "__main__":
    raise SystemExit(main())
