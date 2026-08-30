from __future__ import annotations

import hashlib
import json
import logging
import secrets
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from threading import Condition, Lock

from mvp_orbit.core.logging import log_kv
from mvp_orbit.core.models import (
    ChannelRecord,
    ClientControlEvent,
    ClientEvent,
    ClientRecord,
    CommandCreateRequest,
    CommandLease,
    CommandOutputChunk,
    CommandRecord,
    CommandStatus,
    EventRecord,
    FilePullRequest,
    FilePushRequest,
    FileTransferRecord,
    FileTransferResult,
    FileTransferStatus,
    JoinApprovalRecord,
    JoinRequestStatus,
    JoinResponse,
    ShellSessionCreateRequest,
    ShellSessionLease,
    ShellSessionRecord,
    ShellSessionStatus,
    TokenResponse,
    utc_now,
)

TOKEN_TTL = timedelta(days=7)

logger = logging.getLogger(__name__)


class InvalidTokenError(Exception):
    pass


class ExpiredTokenError(Exception):
    pass


class MembershipError(Exception):
    pass


@dataclass(frozen=True)
class AuthenticatedMember:
    channel_id: str
    expires_at: datetime
    # None for tokens minted before member management existed; such tokens can
    # use everything except the member-management endpoints.
    alias: str | None = None


class HubStore:
    def __init__(self, db_path: str | Path, object_root: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.object_root = Path(object_root)
        self.object_root.mkdir(parents=True, exist_ok=True)
        self.commands_root = self.object_root / "commands"
        self.commands_root.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()
        self._updates = Condition()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS channels (
                    channel_id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS member_tokens (
                    token_hash TEXT PRIMARY KEY,
                    channel_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    revoked_at TEXT
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS channel_members (
                    channel_id TEXT NOT NULL,
                    alias TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (channel_id, alias)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS join_requests (
                    request_id TEXT PRIMARY KEY,
                    channel_id TEXT NOT NULL,
                    alias TEXT NOT NULL,
                    status TEXT NOT NULL,
                    requested_at TEXT NOT NULL,
                    approved_at TEXT,
                    approved_by TEXT,
                    rejected_at TEXT,
                    rejected_by TEXT
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS clients (
                    client_id TEXT PRIMARY KEY,
                    channel_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    last_seen_at TEXT
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS commands (
                    command_id TEXT PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    argv TEXT NOT NULL,
                    env_patch TEXT NOT NULL,
                    timeout_sec INTEGER NOT NULL,
                    working_dir TEXT NOT NULL,
                    claim_timeout_sec INTEGER,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    heartbeat_at TEXT,
                    cancel_requested_at TEXT,
                    exit_code INTEGER,
                    failure_code TEXT,
                    stdout_path TEXT NOT NULL,
                    stderr_path TEXT NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS shell_sessions (
                    session_id TEXT PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    cwd_root TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    heartbeat_at TEXT,
                    close_requested_at TEXT,
                    exit_code INTEGER,
                    failure_code TEXT
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS client_control_events (
                    client_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (client_id, seq)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS command_events (
                    command_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (command_id, seq)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS shell_stream_events (
                    session_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (session_id, seq)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS file_transfers (
                    transfer_id TEXT PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    remote_path TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    max_bytes INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    failure_code TEXT,
                    data_b64 TEXT
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS file_events (
                    transfer_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (transfer_id, seq)
                )
                """
            )
            self._migrate_schema()

    def _migrate_schema(self) -> None:
        # Databases created before these columns existed are upgraded in place.
        for statement in (
            "ALTER TABLE commands ADD COLUMN claim_timeout_sec INTEGER",
            "ALTER TABLE clients ADD COLUMN stream_connected INTEGER",
            "ALTER TABLE channel_members ADD COLUMN role TEXT NOT NULL DEFAULT 'member'",
            "ALTER TABLE member_tokens ADD COLUMN alias TEXT",
            "ALTER TABLE join_requests ADD COLUMN token_issued_at TEXT",
            "ALTER TABLE join_requests ADD COLUMN claim_secret TEXT",
        ):
            try:
                self._conn.execute(statement)
            except sqlite3.OperationalError:
                pass
        # Channels from before roles existed: the first member has always been
        # the de-facto admin, so make that explicit.
        channels = self._conn.execute(
            """
            SELECT DISTINCT channel_id FROM channel_members cm
            WHERE NOT EXISTS (
                SELECT 1 FROM channel_members WHERE channel_id = cm.channel_id AND role = 'admin'
            )
            """
        ).fetchall()
        for row in channels:
            first = self._conn.execute(
                "SELECT alias FROM channel_members WHERE channel_id = ? ORDER BY created_at ASC, rowid ASC LIMIT 1",
                (row["channel_id"],),
            ).fetchone()
            if first is not None:
                self._conn.execute(
                    "UPDATE channel_members SET role = 'admin' WHERE channel_id = ? AND alias = ?",
                    (row["channel_id"], first["alias"]),
                )

    def wait_for_updates(self, timeout: float) -> bool:
        with self._updates:
            return self._updates.wait(timeout=timeout)

    def request_channel_join(self, *, request_id: str, alias: str, channel: str) -> JoinResponse:
        channel_id = self.channel_id_for_name(channel)
        with self._lock, self._conn:
            if self._channel_member_count_locked(channel_id) == 0:
                self._insert_channel_member_locked(channel_id, alias, role="admin")
                token = self._issue_token_locked(channel_id, alias=alias)
                log_kv(logger, logging.INFO, "join.auto_approved", channel_id=channel_id, alias=alias, role="admin")
                return JoinResponse(
                    status=JoinRequestStatus.APPROVED,
                    alias=alias,
                    channel_id=channel_id,
                    member_token=token.member_token,
                    expires_at=token.expires_at,
                )

            # NOTE deliberately no general unauthenticated fast path for an
            # existing alias: /api/join is anonymous, so handing out a token
            # because the caller *claims* a member's alias would let anyone who
            # knows a channel name + alias mint that member's credentials (an
            # admin's, since roles exist). Re-enrollment goes through approval
            # like any other join; membership and role are preserved.
            #
            # Sole exception: a single-member channel re-enrolling its only
            # member. Nobody else could approve it (pure lockout), and an
            # attacker who knows that channel+alias pair could equally wait for
            # the empty channel to be pruned and take it over fresh.
            if self._channel_member_count_locked(channel_id) == 1:
                sole = self._conn.execute(
                    "SELECT alias FROM channel_members WHERE channel_id = ?",
                    (channel_id,),
                ).fetchone()
                if sole is not None and str(sole["alias"]) == alias:
                    token = self._issue_token_locked(channel_id, alias=alias)
                    log_kv(logger, logging.INFO, "join.sole_member_rejoined", channel_id=channel_id, alias=alias)
                    return JoinResponse(
                        status=JoinRequestStatus.APPROVED,
                        alias=alias,
                        channel_id=channel_id,
                        member_token=token.member_token,
                        expires_at=token.expires_at,
                    )

            now = utc_now()
            claim_secret = secrets.token_urlsafe(16)
            self._conn.execute(
                """
                INSERT INTO join_requests (
                    request_id, channel_id, alias, status, requested_at,
                    approved_at, approved_by, rejected_at, rejected_by, claim_secret
                ) VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, ?)
                """,
                (request_id, channel_id, alias, JoinRequestStatus.PENDING.value, now.isoformat(), claim_secret),
            )
            self._append_join_request_events_locked(channel_id, request_id, alias, now)
            log_kv(logger, logging.INFO, "join.pending", channel_id=channel_id, alias=alias, request_id=request_id)
        self._notify_update()
        return JoinResponse(status=JoinRequestStatus.PENDING, alias=alias, channel_id=channel_id, request_id=request_id, claim_secret=claim_secret)

    def get_join_request_response(self, request_id: str, *, claim_secret: str | None = None) -> JoinResponse | None:
        with self._lock, self._conn:
            row = self._conn.execute("SELECT * FROM join_requests WHERE request_id = ?", (request_id,)).fetchone()
            if row is None:
                return None
            record = self._row_to_join_approval(dict(row))
            if record.status == JoinRequestStatus.APPROVED:
                # This endpoint is anonymous, so the member token is released
                # only to whoever holds the claim_secret issued with the
                # request. Retries are then safe (a lost response can simply be
                # re-polled) while a request id scraped from logs mints
                # nothing — including requests created before claim secrets
                # existed, which can no longer mint at all.
                stored_secret = row["claim_secret"]
                if stored_secret is not None and claim_secret is not None and secrets.compare_digest(str(stored_secret), claim_secret):
                    token = self._issue_token_locked(record.channel_id, alias=record.alias)
                    return JoinResponse(
                        status=record.status,
                        alias=record.alias,
                        channel_id=record.channel_id,
                        request_id=record.request_id,
                        member_token=token.member_token,
                        expires_at=token.expires_at,
                    )
                # No anonymous fallback, even for requests created before claim
                # secrets existed: those requesters simply run `orbit join`
                # again. A request id alone must never mint a credential.
                log_kv(logger, logging.WARNING, "join.token_claim_denied", request_id=request_id, alias=record.alias)
            return JoinResponse(status=record.status, alias=record.alias, channel_id=record.channel_id, request_id=record.request_id)

    def list_join_requests(self, channel_id: str, status_filter: JoinRequestStatus | None = None) -> list[JoinApprovalRecord]:
        query = "SELECT * FROM join_requests WHERE channel_id = ?"
        params: list[str] = [channel_id]
        if status_filter is not None:
            query += " AND status = ?"
            params.append(status_filter.value)
        query += " ORDER BY requested_at ASC"
        with self._lock:
            rows = self._conn.execute(query, params).fetchall()
        return [self._row_to_join_approval(dict(row)) for row in rows]

    def approve_join_request(self, request_id: str, approver_channel_id: str) -> JoinApprovalRecord:
        now = utc_now().isoformat()
        with self._lock, self._conn:
            row = self._conn.execute("SELECT * FROM join_requests WHERE request_id = ?", (request_id,)).fetchone()
            if row is None:
                raise KeyError(request_id)
            record = self._row_to_join_approval(dict(row))
            if record.channel_id != approver_channel_id:
                raise MembershipError(request_id)
            if record.status == JoinRequestStatus.PENDING:
                self._conn.execute(
                    """
                    UPDATE join_requests
                    SET status = ?, approved_at = ?, approved_by = ?
                    WHERE request_id = ?
                    """,
                    (JoinRequestStatus.APPROVED.value, now, approver_channel_id, request_id),
                )
                self._insert_channel_member_locked(record.channel_id, record.alias)
                log_kv(logger, logging.INFO, "join.approved", channel_id=record.channel_id, alias=record.alias, request_id=request_id)
            updated = self._conn.execute("SELECT * FROM join_requests WHERE request_id = ?", (request_id,)).fetchone()
            assert updated is not None
        self._notify_update()
        return self._row_to_join_approval(dict(updated))

    def reject_join_request(self, request_id: str, approver_channel_id: str) -> JoinApprovalRecord:
        now = utc_now().isoformat()
        with self._lock, self._conn:
            row = self._conn.execute("SELECT * FROM join_requests WHERE request_id = ?", (request_id,)).fetchone()
            if row is None:
                raise KeyError(request_id)
            record = self._row_to_join_approval(dict(row))
            if record.channel_id != approver_channel_id:
                raise MembershipError(request_id)
            if record.status == JoinRequestStatus.PENDING:
                self._conn.execute(
                    """
                    UPDATE join_requests
                    SET status = ?, rejected_at = ?, rejected_by = ?
                    WHERE request_id = ?
                    """,
                    (JoinRequestStatus.REJECTED.value, now, approver_channel_id, request_id),
                )
                log_kv(logger, logging.INFO, "join.rejected", channel_id=record.channel_id, alias=record.alias, request_id=request_id)
            updated = self._conn.execute("SELECT * FROM join_requests WHERE request_id = ?", (request_id,)).fetchone()
            assert updated is not None
        self._notify_update()
        return self._row_to_join_approval(dict(updated))

    def authenticate_member_token(self, member_token: str) -> AuthenticatedMember:
        token_hash = self._hash_token(member_token)
        with self._lock:
            row = self._conn.execute(
                "SELECT channel_id, expires_at, revoked_at, alias FROM member_tokens WHERE token_hash = ?",
                (token_hash,),
            ).fetchone()
        if row is None or row["revoked_at"] is not None:
            raise InvalidTokenError("invalid token")
        expires_at = _parse_dt(row["expires_at"])
        assert expires_at is not None
        if expires_at <= utc_now():
            raise ExpiredTokenError("token expired")
        alias = row["alias"]
        return AuthenticatedMember(channel_id=str(row["channel_id"]), expires_at=expires_at, alias=str(alias) if alias else None)

    def ensure_channel(self, channel_id: str) -> ChannelRecord:
        now = utc_now()
        with self._lock, self._conn:
            self._ensure_channel_locked(channel_id)
            row = self._conn.execute("SELECT * FROM channels WHERE channel_id = ?", (channel_id,)).fetchone()
            assert row is not None
        return self._row_to_channel(dict(row))

    def register_client(self, client_id: str, channel_id: str) -> ClientRecord:
        now = utc_now().isoformat()
        with self._lock, self._conn:
            row = self._conn.execute("SELECT * FROM clients WHERE client_id = ?", (client_id,)).fetchone()
            if row is None:
                self._conn.execute(
                    """
                    INSERT INTO clients (client_id, channel_id, created_at, last_seen_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (client_id, channel_id, now, now),
                )
            else:
                if row["channel_id"] != channel_id:
                    raise MembershipError(client_id)
                self._conn.execute("UPDATE clients SET last_seen_at = ? WHERE client_id = ?", (now, client_id))
            updated = self._conn.execute("SELECT * FROM clients WHERE client_id = ?", (client_id,)).fetchone()
            assert updated is not None
        return self._row_to_client(dict(updated))

    def get_client(self, client_id: str) -> ClientRecord | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM clients WHERE client_id = ?", (client_id,)).fetchone()
        if row is None:
            return None
        return self._row_to_client(dict(row))

    def list_clients(self, channel_id: str) -> list[ClientRecord]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM clients WHERE channel_id = ? ORDER BY client_id ASC",
                (channel_id,),
            ).fetchall()
        return [self._row_to_client(dict(row)) for row in rows]

    def assert_client_member(self, client_id: str, channel_id: str) -> None:
        client = self.get_client(client_id)
        if client is None or client.channel_id != channel_id:
            raise MembershipError(client_id)

    def create_command(self, command_id: str, channel_id: str, request: CommandCreateRequest) -> CommandRecord:
        stdout_path = self.command_output_path(command_id, "stdout")
        stderr_path = self.command_output_path(command_id, "stderr")
        stdout_path.write_text("", encoding="utf-8")
        stderr_path.write_text("", encoding="utf-8")
        now = utc_now()
        record = CommandRecord(
            command_id=command_id,
            client_id=request.client_id,
            channel_id=channel_id,
            argv=request.argv,
            env_patch=request.env_patch,
            timeout_sec=request.timeout_sec,
            working_dir=request.working_dir,
            claim_timeout_sec=request.claim_timeout_sec,
            status=CommandStatus.QUEUED,
            created_at=now,
            stdout_path=str(stdout_path),
            stderr_path=str(stderr_path),
        )
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO commands (
                    command_id, client_id, channel_id, argv, env_patch, timeout_sec, working_dir,
                    claim_timeout_sec, status, created_at, started_at, finished_at, heartbeat_at,
                    cancel_requested_at, exit_code, failure_code, stdout_path, stderr_path
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                self._command_values(record),
            )
            self._append_client_control_event_locked(request.client_id, "command.start", {"command_id": command_id})
        self._notify_update()
        return record

    def claim_command(self, command_id: str) -> CommandLease:
        with self._lock, self._conn:
            row = self._conn.execute("SELECT * FROM commands WHERE command_id = ?", (command_id,)).fetchone()
            if row is None:
                raise KeyError(command_id)
            record = self._row_to_command(dict(row))
            if record.status != CommandStatus.QUEUED:
                raise ValueError(f"command not claimable: {record.status.value}")
            now = utc_now().isoformat()
            self._conn.execute(
                "UPDATE commands SET status = ?, started_at = ?, heartbeat_at = ? WHERE command_id = ?",
                (CommandStatus.RUNNING.value, now, now, command_id),
            )
            updated = self._conn.execute("SELECT * FROM commands WHERE command_id = ?", (command_id,)).fetchone()
            assert updated is not None
        record = self._row_to_command(dict(updated))
        return CommandLease(
            command_id=record.command_id,
            client_id=record.client_id,
            argv=record.argv,
            env_patch=record.env_patch,
            timeout_sec=record.timeout_sec,
            working_dir=record.working_dir,
        )

    def cancel_command(self, command_id: str) -> CommandRecord:
        now = utc_now().isoformat()
        with self._lock, self._conn:
            row = self._conn.execute("SELECT * FROM commands WHERE command_id = ?", (command_id,)).fetchone()
            if row is None:
                raise KeyError(command_id)
            record = self._row_to_command(dict(row))
            if record.status in {CommandStatus.SUCCEEDED, CommandStatus.FAILED, CommandStatus.CANCELED}:
                return record
            if record.status == CommandStatus.QUEUED:
                self._conn.execute(
                    """
                    UPDATE commands
                    SET status = ?, cancel_requested_at = ?, finished_at = ?, exit_code = ?, failure_code = ?
                    WHERE command_id = ?
                    """,
                    (CommandStatus.CANCELED.value, now, now, -15, "canceled", command_id),
                )
                payload = {"command_id": command_id, "status": CommandStatus.CANCELED.value, "exit_code": -15, "failure_code": "canceled"}
                self._append_command_event_locked(command_id, "command.exit", payload)
            else:
                self._conn.execute("UPDATE commands SET cancel_requested_at = ? WHERE command_id = ?", (now, command_id))
                self._append_client_control_event_locked(record.client_id, "command.cancel", {"command_id": command_id})
            updated = self._conn.execute("SELECT * FROM commands WHERE command_id = ?", (command_id,)).fetchone()
            assert updated is not None
        self._notify_update()
        return self._row_to_command(dict(updated))

    def get_command(self, command_id: str) -> CommandRecord | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM commands WHERE command_id = ?", (command_id,)).fetchone()
        if row is None:
            return None
        return self._row_to_command(dict(row))

    def read_command_output(self, command_id: str, *, stdout_offset: int = 0, stderr_offset: int = 0) -> CommandOutputChunk:
        record = self.get_command(command_id)
        if record is None:
            raise KeyError(command_id)
        stdout = self._read_from_offset(self.command_output_path(command_id, "stdout"), stdout_offset)
        stderr = self._read_from_offset(self.command_output_path(command_id, "stderr"), stderr_offset)
        return CommandOutputChunk(
            command_id=command_id,
            status=record.status,
            stdout=stdout,
            stderr=stderr,
            stdout_offset=stdout_offset + len(stdout),
            stderr_offset=stderr_offset + len(stderr),
            exit_code=record.exit_code,
            failure_code=record.failure_code,
        )

    def create_shell_session(self, session_id: str, channel_id: str, request: ShellSessionCreateRequest, cwd_root: str) -> ShellSessionRecord:
        record = ShellSessionRecord(
            session_id=session_id,
            client_id=request.client_id,
            channel_id=channel_id,
            cwd_root=cwd_root,
            status=ShellSessionStatus.QUEUED,
            created_at=utc_now(),
        )
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO shell_sessions (
                    session_id, client_id, channel_id, cwd_root, status, created_at,
                    started_at, finished_at, heartbeat_at, close_requested_at, exit_code, failure_code
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                self._shell_values(record),
            )
            self._append_client_control_event_locked(request.client_id, "shell.start", {"session_id": session_id})
        self._notify_update()
        return record

    def claim_shell_session(self, session_id: str) -> ShellSessionLease:
        with self._lock, self._conn:
            row = self._conn.execute("SELECT * FROM shell_sessions WHERE session_id = ?", (session_id,)).fetchone()
            if row is None:
                raise KeyError(session_id)
            record = self._row_to_shell(dict(row))
            if record.status != ShellSessionStatus.QUEUED:
                raise ValueError(f"shell not claimable: {record.status.value}")
            now = utc_now().isoformat()
            self._conn.execute(
                "UPDATE shell_sessions SET status = ?, started_at = ?, heartbeat_at = ? WHERE session_id = ?",
                (ShellSessionStatus.RUNNING.value, now, now, session_id),
            )
            updated = self._conn.execute("SELECT * FROM shell_sessions WHERE session_id = ?", (session_id,)).fetchone()
            assert updated is not None
        record = self._row_to_shell(dict(updated))
        return ShellSessionLease(session_id=record.session_id, client_id=record.client_id, cwd_root=record.cwd_root)

    def append_shell_input(self, session_id: str, data: str) -> int:
        with self._lock, self._conn:
            row = self._conn.execute("SELECT * FROM shell_sessions WHERE session_id = ?", (session_id,)).fetchone()
            if row is None:
                raise KeyError(session_id)
            record = self._row_to_shell(dict(row))
            event = self._append_client_control_event_locked(record.client_id, "shell.stdin", {"session_id": session_id, "data": data})
        self._notify_update()
        return event.event_id

    def resize_shell_session(self, session_id: str, rows: int, cols: int) -> int:
        with self._lock, self._conn:
            row = self._conn.execute("SELECT * FROM shell_sessions WHERE session_id = ?", (session_id,)).fetchone()
            if row is None:
                raise KeyError(session_id)
            record = self._row_to_shell(dict(row))
            event = self._append_client_control_event_locked(record.client_id, "shell.resize", {"session_id": session_id, "rows": rows, "cols": cols})
        self._notify_update()
        return event.event_id

    def close_shell_session(self, session_id: str) -> ShellSessionRecord:
        now = utc_now().isoformat()
        with self._lock, self._conn:
            row = self._conn.execute("SELECT * FROM shell_sessions WHERE session_id = ?", (session_id,)).fetchone()
            if row is None:
                raise KeyError(session_id)
            record = self._row_to_shell(dict(row))
            if record.status in {ShellSessionStatus.CLOSED, ShellSessionStatus.FAILED}:
                return record
            if record.status == ShellSessionStatus.QUEUED:
                self._conn.execute(
                    """
                    UPDATE shell_sessions
                    SET status = ?, close_requested_at = ?, finished_at = ?, exit_code = ?, failure_code = ?
                    WHERE session_id = ?
                    """,
                    (ShellSessionStatus.CLOSED.value, now, now, 0, None, session_id),
                )
                self._append_shell_event_locked(session_id, "shell.closed", {"session_id": session_id, "status": ShellSessionStatus.CLOSED.value, "exit_code": 0})
            else:
                self._conn.execute("UPDATE shell_sessions SET close_requested_at = ? WHERE session_id = ?", (now, session_id))
                self._append_client_control_event_locked(record.client_id, "shell.close", {"session_id": session_id})
            updated = self._conn.execute("SELECT * FROM shell_sessions WHERE session_id = ?", (session_id,)).fetchone()
            assert updated is not None
        self._notify_update()
        return self._row_to_shell(dict(updated))

    def get_shell_session(self, session_id: str) -> ShellSessionRecord | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM shell_sessions WHERE session_id = ?", (session_id,)).fetchone()
        if row is None:
            return None
        return self._row_to_shell(dict(row))

    def list_shell_sessions(self, channel_id: str, *, client_id: str | None = None, session_status: ShellSessionStatus | None = None) -> list[ShellSessionRecord]:
        query = "SELECT * FROM shell_sessions WHERE channel_id = ?"
        params: list[str] = [channel_id]
        if client_id is not None:
            query += " AND client_id = ?"
            params.append(client_id)
        if session_status is not None:
            query += " AND status = ?"
            params.append(session_status.value)
        query += " ORDER BY created_at DESC"
        with self._lock:
            rows = self._conn.execute(query, params).fetchall()
        return [self._row_to_shell(dict(row)) for row in rows]

    def get_client_control_events(self, client_id: str, after_seq: int) -> list[ClientControlEvent]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT seq, kind, payload_json, created_at
                FROM client_control_events
                WHERE client_id = ? AND seq > ?
                ORDER BY seq ASC
                """,
                (client_id, after_seq),
            ).fetchall()
        return [
            ClientControlEvent(
                event_id=int(row["seq"]),
                client_id=client_id,
                kind=str(row["kind"]),
                payload=json.loads(str(row["payload_json"])),
                created_at=_parse_dt(row["created_at"]),
            )
            for row in rows
        ]

    def get_command_events(self, command_id: str, after_seq: int) -> list[EventRecord]:
        return self._list_events("command_events", "command_id", command_id, after_seq)

    def get_shell_events(self, session_id: str, after_seq: int) -> list[EventRecord]:
        return self._list_events("shell_stream_events", "session_id", session_id, after_seq)

    def get_file_events(self, transfer_id: str, after_seq: int) -> list[EventRecord]:
        return self._list_events("file_events", "transfer_id", transfer_id, after_seq)

    def append_command_event(self, command_id: str, kind: str, payload: dict) -> EventRecord:
        with self._lock, self._conn:
            event = self._append_command_event_locked(command_id, kind, payload)
        self._notify_update()
        return event

    def append_shell_event(self, session_id: str, kind: str, payload: dict) -> EventRecord:
        with self._lock, self._conn:
            event = self._append_shell_event_locked(session_id, kind, payload)
        self._notify_update()
        return event

    def apply_client_events(self, client_id: str, events: list[ClientEvent]) -> None:
        if not events:
            return
        with self._lock, self._conn:
            for item in events:
                kind = item.kind
                payload = dict(item.payload)
                if kind == "client.reconcile":
                    active_commands = set(payload.get("active_command_ids") or [])
                    active_shells = set(payload.get("active_session_ids") or [])
                    now = utc_now().isoformat()
                    for row in self._conn.execute(
                        "SELECT command_id FROM commands WHERE client_id = ? AND status = ?",
                        (client_id, CommandStatus.RUNNING.value),
                    ).fetchall():
                        command_id = str(row["command_id"])
                        if command_id in active_commands:
                            continue
                        self._conn.execute(
                            "UPDATE commands SET status = ?, finished_at = ?, failure_code = ? WHERE command_id = ? AND status = ?",
                            (CommandStatus.FAILED.value, now, "client_restarted", command_id, CommandStatus.RUNNING.value),
                        )
                        self._append_command_event_locked(
                            command_id,
                            "command.exit",
                            {"command_id": command_id, "status": CommandStatus.FAILED.value, "exit_code": None, "failure_code": "client_restarted"},
                        )
                        log_kv(logger, logging.WARNING, "command.client_restarted", command_id=command_id, client_id=client_id)
                    for row in self._conn.execute(
                        "SELECT session_id FROM shell_sessions WHERE client_id = ? AND status = ?",
                        (client_id, ShellSessionStatus.RUNNING.value),
                    ).fetchall():
                        session_id = str(row["session_id"])
                        if session_id in active_shells:
                            continue
                        self._conn.execute(
                            "UPDATE shell_sessions SET status = ?, finished_at = ?, failure_code = ? WHERE session_id = ? AND status = ?",
                            (ShellSessionStatus.FAILED.value, now, "client_restarted", session_id, ShellSessionStatus.RUNNING.value),
                        )
                        self._append_shell_event_locked(
                            session_id,
                            "shell.exit",
                            {"session_id": session_id, "status": ShellSessionStatus.FAILED.value, "exit_code": None, "failure_code": "client_restarted"},
                        )
                        log_kv(logger, logging.WARNING, "shell.client_restarted", session_id=session_id, client_id=client_id)
                    continue
                if kind == "client.heartbeat":
                    now = utc_now().isoformat()
                    stream_connected = payload.get("stream_connected")
                    if stream_connected is None:
                        self._conn.execute("UPDATE clients SET last_seen_at = ? WHERE client_id = ?", (now, client_id))
                    else:
                        self._conn.execute(
                            "UPDATE clients SET last_seen_at = ?, stream_connected = ? WHERE client_id = ?",
                            (now, 1 if stream_connected else 0, client_id),
                        )
                    continue
                if kind == "command.started":
                    self._append_command_event_locked(payload["command_id"], kind, payload)
                    continue
                if kind in {"command.stdout", "command.stderr"}:
                    command_id = str(payload["command_id"])
                    data = str(payload.get("data", ""))
                    path = self.command_output_path(command_id, "stdout" if kind.endswith("stdout") else "stderr")
                    with path.open("a", encoding="utf-8") as handle:
                        handle.write(data)
                    self._append_command_event_locked(command_id, kind, payload)
                    continue
                if kind == "command.exit":
                    command_id = str(payload["command_id"])
                    finished = utc_now().isoformat()
                    updated = self._conn.execute(
                        """
                        UPDATE commands
                        SET status = ?, finished_at = ?, exit_code = ?, failure_code = ?
                        WHERE command_id = ? AND status IN (?, ?)
                        """,
                        (
                            payload["status"],
                            finished,
                            payload.get("exit_code"),
                            payload.get("failure_code"),
                            command_id,
                            CommandStatus.QUEUED.value,
                            CommandStatus.RUNNING.value,
                        ),
                    )
                    if updated.rowcount:
                        self._append_command_event_locked(command_id, kind, payload)
                    else:
                        # Already terminal (e.g. reaped as unclaimed, or canceled):
                        # a late completion must not resurrect the record.
                        log_kv(logger, logging.INFO, "command.exit_ignored", command_id=command_id, reported=payload.get("status"))
                    continue
                if kind == "shell.started":
                    self._append_shell_event_locked(payload["session_id"], kind, payload)
                    continue
                if kind in {"shell.stdout", "shell.stderr", "shell.system"}:
                    self._append_shell_event_locked(payload["session_id"], kind, payload)
                    continue
                if kind in {"shell.exit", "shell.closed"}:
                    session_id = str(payload["session_id"])
                    finished = utc_now().isoformat()
                    updated = self._conn.execute(
                        """
                        UPDATE shell_sessions
                        SET status = ?, finished_at = ?, exit_code = ?, failure_code = ?
                        WHERE session_id = ? AND status IN (?, ?)
                        """,
                        (
                            payload["status"],
                            finished,
                            payload.get("exit_code"),
                            payload.get("failure_code"),
                            session_id,
                            ShellSessionStatus.QUEUED.value,
                            ShellSessionStatus.RUNNING.value,
                        ),
                    )
                    if updated.rowcount:
                        self._append_shell_event_locked(session_id, kind, payload)
                    continue
                if kind == "file.started":
                    transfer_id = str(payload["transfer_id"])
                    now = utc_now().isoformat()
                    self._conn.execute(
                        "UPDATE file_transfers SET status = ?, started_at = ? WHERE transfer_id = ? AND status = ?",
                        (FileTransferStatus.RUNNING.value, now, transfer_id, FileTransferStatus.QUEUED.value),
                    )
                    continue
                if kind == "file.result":
                    result = FileTransferResult.model_validate(payload)
                    finished = utc_now().isoformat()
                    updated = self._conn.execute(
                        """
                        UPDATE file_transfers
                        SET status = ?, finished_at = ?, failure_code = ?, data_b64 = COALESCE(?, data_b64), size = ?
                        WHERE transfer_id = ? AND status IN (?, ?)
                        """,
                        (
                            result.status.value,
                            finished,
                            result.failure_code,
                            result.data_b64,
                            result.size,
                            result.transfer_id,
                            FileTransferStatus.QUEUED.value,
                            FileTransferStatus.RUNNING.value,
                        ),
                    )
                    if updated.rowcount:
                        self._append_file_event_locked(result.transfer_id, kind, result.model_dump(mode="json"))
                    continue
                raise ValueError(f"unsupported client event kind: {kind}")
        self._notify_update()

    def create_file_push(self, transfer_id: str, channel_id: str, request: FilePushRequest, size: int) -> FileTransferRecord:
        if size > request.max_bytes:
            raise ValueError("file exceeds max_bytes")
        now = utc_now()
        record = FileTransferRecord(
            transfer_id=transfer_id,
            client_id=request.client_id,
            channel_id=channel_id,
            direction="push",
            remote_path=request.remote_path,
            size=size,
            max_bytes=request.max_bytes,
            status=FileTransferStatus.QUEUED,
            created_at=now,
            data_b64=request.data_b64,
        )
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO file_transfers (
                    transfer_id, client_id, channel_id, direction, remote_path, size, max_bytes, status,
                    created_at, started_at, finished_at, failure_code, data_b64
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                self._file_transfer_values(record),
            )
            self._append_client_control_event_locked(
                request.client_id,
                "file.push",
                {"transfer_id": transfer_id, "remote_path": request.remote_path, "data_b64": request.data_b64, "max_bytes": request.max_bytes},
            )
        self._notify_update()
        return record

    def create_file_pull(self, transfer_id: str, channel_id: str, request: FilePullRequest) -> FileTransferRecord:
        now = utc_now()
        record = FileTransferRecord(
            transfer_id=transfer_id,
            client_id=request.client_id,
            channel_id=channel_id,
            direction="pull",
            remote_path=request.remote_path,
            size=0,
            max_bytes=request.max_bytes,
            status=FileTransferStatus.QUEUED,
            created_at=now,
        )
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO file_transfers (
                    transfer_id, client_id, channel_id, direction, remote_path, size, max_bytes, status,
                    created_at, started_at, finished_at, failure_code, data_b64
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                self._file_transfer_values(record),
            )
            self._append_client_control_event_locked(
                request.client_id,
                "file.pull",
                {"transfer_id": transfer_id, "remote_path": request.remote_path, "max_bytes": request.max_bytes},
            )
        self._notify_update()
        return record

    def get_file_transfer(self, transfer_id: str) -> FileTransferRecord | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM file_transfers WHERE transfer_id = ?", (transfer_id,)).fetchone()
        if row is None:
            return None
        return self._row_to_file_transfer(dict(row))

    def renew_member_token(self, channel_id: str, alias: str) -> TokenResponse:
        """Mint a fresh token for a member who presented a still-valid one.

        Holding an unexpired token IS the proof of identity, so this needs no
        approval — it is what keeps long-lived machines from hitting the 7-day
        expiry (which, with approval-required re-enrollment, could deadlock a
        channel if every member expired at once). The old token is not revoked;
        it simply ages out, so a lost renewal response is harmless."""
        with self._lock, self._conn:
            if self._member_role_locked(channel_id, alias) is None:
                raise MembershipError(f"{alias!r} is no longer a member of this channel")
            token = self._issue_token_locked(channel_id, alias=alias)
            log_kv(logger, logging.INFO, "member.token_renewed", channel_id=channel_id, alias=alias)
        return token

    def list_members(self, channel_id: str) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT m.alias, m.role, m.created_at AS joined_at,
                       c.last_seen_at, c.stream_connected
                FROM channel_members m
                LEFT JOIN clients c ON c.channel_id = m.channel_id AND c.client_id = m.alias
                WHERE m.channel_id = ?
                ORDER BY m.created_at ASC, m.rowid ASC
                """,
                (channel_id,),
            ).fetchall()
        members = []
        for row in rows:
            stream_connected = row["stream_connected"]
            members.append(
                {
                    "alias": row["alias"],
                    "role": row["role"],
                    "joined_at": row["joined_at"],
                    "last_seen_at": row["last_seen_at"],
                    "stream_connected": None if stream_connected is None else bool(stream_connected),
                }
            )
        return members

    def _member_role_locked(self, channel_id: str, alias: str) -> str | None:
        row = self._conn.execute(
            "SELECT role FROM channel_members WHERE channel_id = ? AND alias = ?",
            (channel_id, alias),
        ).fetchone()
        return None if row is None else str(row["role"])

    def _admin_count_locked(self, channel_id: str) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) AS count FROM channel_members WHERE channel_id = ? AND role = 'admin'",
            (channel_id,),
        ).fetchone()
        return int(row["count"])

    def _evict_member_locked(self, channel_id: str, alias: str) -> None:
        # Revoking the tokens takes effect on the member's very next request.
        # Legacy tokens (minted before alias binding) cannot be attributed to a
        # member, so evicting anyone also revokes all of them — otherwise the
        # evicted member could keep using a pre-v0.7 token forever. Remaining
        # members holding such tokens just re-enroll once.
        now = utc_now().isoformat()
        self._conn.execute(
            "UPDATE member_tokens SET revoked_at = ? WHERE channel_id = ? AND (alias = ? OR alias IS NULL) AND revoked_at IS NULL",
            (now, channel_id, alias),
        )
        client_ids = [
            str(row["client_id"])
            for row in self._conn.execute(
                "SELECT client_id FROM clients WHERE channel_id = ? AND client_id = ?",
                (channel_id, alias),
            ).fetchall()
        ]
        self._delete_where_in_locked("client_control_events", "client_id", client_ids)
        self._conn.execute("DELETE FROM clients WHERE channel_id = ? AND client_id = ?", (channel_id, alias))
        self._conn.execute("DELETE FROM channel_members WHERE channel_id = ? AND alias = ?", (channel_id, alias))

    def leave_channel(self, channel_id: str, alias: str) -> dict:
        with self._lock, self._conn:
            role = self._member_role_locked(channel_id, alias)
            if role is None:
                raise MembershipError(f"{alias!r} is not a member of this channel")
            member_count = self._channel_member_count_locked(channel_id)
            if member_count > 1 and role == "admin" and self._admin_count_locked(channel_id) == 1:
                raise ValueError("you are the only admin — run `orbit transfer-admin <alias>` before leaving")
            self._evict_member_locked(channel_id, alias)
            channel_deleted = member_count == 1
            if channel_deleted:
                self._delete_channel_locked(channel_id)
            log_kv(logger, logging.INFO, "member.left", channel_id=channel_id, alias=alias, channel_deleted=channel_deleted)
        self._notify_update()
        return {"status": "left", "alias": alias, "channel_id": channel_id, "channel_deleted": channel_deleted}

    def remove_member(self, channel_id: str, actor_alias: str, target_alias: str) -> dict:
        with self._lock, self._conn:
            if self._member_role_locked(channel_id, actor_alias) != "admin":
                raise MembershipError("admin role required to remove members")
            if actor_alias == target_alias:
                raise ValueError("cannot remove yourself — use `orbit leave`")
            if self._member_role_locked(channel_id, target_alias) is None:
                raise KeyError(target_alias)
            self._evict_member_locked(channel_id, target_alias)
            log_kv(logger, logging.INFO, "member.removed", channel_id=channel_id, alias=target_alias, by=actor_alias)
        self._notify_update()
        return {"status": "removed", "alias": target_alias, "channel_id": channel_id, "channel_deleted": False}

    def set_member_role(self, channel_id: str, actor_alias: str, target_alias: str, role: str) -> dict:
        if role not in {"admin", "member"}:
            raise ValueError(f"unknown role {role!r}")
        with self._lock, self._conn:
            if self._member_role_locked(channel_id, actor_alias) != "admin":
                raise MembershipError("admin role required to change roles")
            if self._member_role_locked(channel_id, target_alias) is None:
                raise KeyError(target_alias)
            if role == "member" and actor_alias == target_alias and self._admin_count_locked(channel_id) == 1:
                raise ValueError("you are the only admin — promote someone else first")
            self._conn.execute(
                "UPDATE channel_members SET role = ? WHERE channel_id = ? AND alias = ?",
                (role, channel_id, target_alias),
            )
            log_kv(logger, logging.INFO, "member.role_changed", channel_id=channel_id, alias=target_alias, role=role, by=actor_alias)
        self._notify_update()
        return {"status": "role_changed", "alias": target_alias, "channel_id": channel_id, "role": role}

    def reap_unclaimed_work(self, *, default_claim_timeout_sec: float, client_lost_after_sec: float = 90.0) -> int:
        """Fail QUEUED work nobody claimed in time, so initiators get a terminal
        event instead of hanging forever on a dead or absent peer. Also fail
        RUNNING work whose executing client stopped heartbeating (crashed or
        was removed mid-command) — the client can no longer report an exit."""
        now = utc_now()
        reaped = 0
        with self._lock, self._conn:
            if client_lost_after_sec > 0:
                lost_cutoff = (now - timedelta(seconds=client_lost_after_sec)).isoformat()
                lost_rows = self._conn.execute(
                    """
                    SELECT cmd.* FROM commands cmd
                    LEFT JOIN clients c ON c.client_id = cmd.client_id AND c.channel_id = cmd.channel_id
                    WHERE cmd.status = ? AND (c.client_id IS NULL OR c.last_seen_at IS NULL OR c.last_seen_at < ?)
                    """,
                    (CommandStatus.RUNNING.value, lost_cutoff),
                ).fetchall()
                for row in lost_rows:
                    record = self._row_to_command(dict(row))
                    self._conn.execute(
                        "UPDATE commands SET status = ?, finished_at = ?, failure_code = ? WHERE command_id = ? AND status = ?",
                        (CommandStatus.FAILED.value, now.isoformat(), "client_lost", record.command_id, CommandStatus.RUNNING.value),
                    )
                    self._append_command_event_locked(
                        record.command_id,
                        "command.exit",
                        {"command_id": record.command_id, "status": CommandStatus.FAILED.value, "exit_code": None, "failure_code": "client_lost"},
                    )
                    log_kv(logger, logging.WARNING, "command.client_lost", command_id=record.command_id, client_id=record.client_id)
                    reaped += 1
                lost_shells = self._conn.execute(
                    """
                    SELECT s.* FROM shell_sessions s
                    LEFT JOIN clients c ON c.client_id = s.client_id AND c.channel_id = s.channel_id
                    WHERE s.status = ? AND (c.client_id IS NULL OR c.last_seen_at IS NULL OR c.last_seen_at < ?)
                    """,
                    (ShellSessionStatus.RUNNING.value, lost_cutoff),
                ).fetchall()
                for row in lost_shells:
                    record = self._row_to_shell(dict(row))
                    self._conn.execute(
                        "UPDATE shell_sessions SET status = ?, finished_at = ?, failure_code = ? WHERE session_id = ? AND status = ?",
                        (ShellSessionStatus.FAILED.value, now.isoformat(), "client_lost", record.session_id, ShellSessionStatus.RUNNING.value),
                    )
                    self._append_shell_event_locked(
                        record.session_id,
                        "shell.exit",
                        {"session_id": record.session_id, "status": ShellSessionStatus.FAILED.value, "exit_code": None, "failure_code": "client_lost"},
                    )
                    log_kv(logger, logging.WARNING, "shell.client_lost", session_id=record.session_id, client_id=record.client_id)
                    reaped += 1
            for row in self._conn.execute("SELECT * FROM commands WHERE status = ?", (CommandStatus.QUEUED.value,)).fetchall():
                record = self._row_to_command(dict(row))
                timeout = record.claim_timeout_sec if record.claim_timeout_sec is not None else default_claim_timeout_sec
                if timeout <= 0 or now - record.created_at < timedelta(seconds=timeout):
                    continue
                self._conn.execute(
                    "UPDATE commands SET status = ?, finished_at = ?, failure_code = ? WHERE command_id = ? AND status = ?",
                    (CommandStatus.FAILED.value, now.isoformat(), "unclaimed", record.command_id, CommandStatus.QUEUED.value),
                )
                self._append_command_event_locked(
                    record.command_id,
                    "command.exit",
                    {"command_id": record.command_id, "status": CommandStatus.FAILED.value, "exit_code": None, "failure_code": "unclaimed"},
                )
                log_kv(logger, logging.WARNING, "command.unclaimed", command_id=record.command_id, client_id=record.client_id, timeout_sec=timeout)
                reaped += 1

            if default_claim_timeout_sec > 0:
                cutoff = (now - timedelta(seconds=default_claim_timeout_sec)).isoformat()
                for row in self._conn.execute(
                    "SELECT * FROM shell_sessions WHERE status = ? AND created_at < ?",
                    (ShellSessionStatus.QUEUED.value, cutoff),
                ).fetchall():
                    record = self._row_to_shell(dict(row))
                    self._conn.execute(
                        "UPDATE shell_sessions SET status = ?, finished_at = ?, failure_code = ? WHERE session_id = ?",
                        (ShellSessionStatus.FAILED.value, now.isoformat(), "unclaimed", record.session_id),
                    )
                    self._append_shell_event_locked(
                        record.session_id,
                        "shell.exit",
                        {"session_id": record.session_id, "status": ShellSessionStatus.FAILED.value, "exit_code": None, "failure_code": "unclaimed"},
                    )
                    log_kv(logger, logging.WARNING, "shell.unclaimed", session_id=record.session_id, client_id=record.client_id)
                    reaped += 1

                for row in self._conn.execute(
                    "SELECT * FROM file_transfers WHERE status = ? AND created_at < ?",
                    (FileTransferStatus.QUEUED.value, cutoff),
                ).fetchall():
                    record = self._row_to_file_transfer(dict(row))
                    self._conn.execute(
                        "UPDATE file_transfers SET status = ?, finished_at = ?, failure_code = ? WHERE transfer_id = ?",
                        (FileTransferStatus.FAILED.value, now.isoformat(), "unclaimed", record.transfer_id),
                    )
                    result = FileTransferResult(
                        transfer_id=record.transfer_id,
                        status=FileTransferStatus.FAILED,
                        direction=record.direction,
                        remote_path=record.remote_path,
                        size=record.size,
                        failure_code="unclaimed",
                    )
                    self._append_file_event_locked(record.transfer_id, "file.result", result.model_dump(mode="json"))
                    log_kv(logger, logging.WARNING, "file.unclaimed", transfer_id=record.transfer_id, client_id=record.client_id)
                    reaped += 1
        if reaped:
            self._notify_update()
        return reaped

    def cleanup_empty_channels(self, *, offline_after_sec: float, empty_ttl_sec: float) -> list[str]:
        now = utc_now()
        online_cutoff = now - timedelta(seconds=max(1.0, offline_after_sec))
        empty_cutoff = now - timedelta(seconds=max(0.0, empty_ttl_sec))
        pruned: list[str] = []
        with self._lock, self._conn:
            rows = self._conn.execute("SELECT channel_id, MAX(created_at) AS last_member_at FROM channel_members GROUP BY channel_id").fetchall()
            for row in rows:
                channel_id = str(row["channel_id"])
                online = self._conn.execute(
                    """
                    SELECT 1 FROM clients
                    WHERE channel_id = ? AND last_seen_at IS NOT NULL AND last_seen_at > ?
                    LIMIT 1
                    """,
                    (channel_id, online_cutoff.isoformat()),
                ).fetchone()
                if online is not None:
                    continue

                last_activity = _parse_dt(row["last_member_at"]) or now
                client_row = self._conn.execute(
                    "SELECT MAX(last_seen_at) AS last_seen_at FROM clients WHERE channel_id = ?",
                    (channel_id,),
                ).fetchone()
                if client_row is not None and client_row["last_seen_at"]:
                    last_activity = max(last_activity, _parse_dt(client_row["last_seen_at"]))
                join_row = self._conn.execute(
                    "SELECT MAX(requested_at) AS requested_at FROM join_requests WHERE channel_id = ?",
                    (channel_id,),
                ).fetchone()
                if join_row is not None and join_row["requested_at"]:
                    last_activity = max(last_activity, _parse_dt(join_row["requested_at"]))
                if last_activity > empty_cutoff:
                    continue

                self._delete_channel_locked(channel_id)
                pruned.append(channel_id)
        if pruned:
            log_kv(logger, logging.INFO, "channels.pruned", count=len(pruned), channels=",".join(pruned))
            self._notify_update()
        return pruned

    def _delete_channel_locked(self, channel_id: str) -> None:
        client_ids = [str(row["client_id"]) for row in self._conn.execute("SELECT client_id FROM clients WHERE channel_id = ?", (channel_id,)).fetchall()]
        command_ids = [str(row["command_id"]) for row in self._conn.execute("SELECT command_id FROM commands WHERE channel_id = ?", (channel_id,)).fetchall()]
        shell_ids = [str(row["session_id"]) for row in self._conn.execute("SELECT session_id FROM shell_sessions WHERE channel_id = ?", (channel_id,)).fetchall()]
        file_ids = [str(row["transfer_id"]) for row in self._conn.execute("SELECT transfer_id FROM file_transfers WHERE channel_id = ?", (channel_id,)).fetchall()]

        for command_id in command_ids:
            for stream in ("stdout", "stderr"):
                try:
                    self.command_output_path(command_id, stream).unlink(missing_ok=True)
                except OSError:
                    pass

        self._delete_where_in_locked("client_control_events", "client_id", client_ids)
        self._delete_where_in_locked("command_events", "command_id", command_ids)
        self._delete_where_in_locked("shell_stream_events", "session_id", shell_ids)
        self._delete_where_in_locked("file_events", "transfer_id", file_ids)
        self._conn.execute("DELETE FROM commands WHERE channel_id = ?", (channel_id,))
        self._conn.execute("DELETE FROM shell_sessions WHERE channel_id = ?", (channel_id,))
        self._conn.execute("DELETE FROM file_transfers WHERE channel_id = ?", (channel_id,))
        self._conn.execute("DELETE FROM clients WHERE channel_id = ?", (channel_id,))
        self._conn.execute("DELETE FROM join_requests WHERE channel_id = ?", (channel_id,))
        self._conn.execute("DELETE FROM channel_members WHERE channel_id = ?", (channel_id,))
        self._conn.execute("DELETE FROM member_tokens WHERE channel_id = ?", (channel_id,))
        self._conn.execute("DELETE FROM channels WHERE channel_id = ?", (channel_id,))

    def _delete_where_in_locked(self, table: str, column: str, values: list[str]) -> None:
        if not values:
            return
        placeholders = ",".join("?" for _ in values)
        self._conn.execute(f"DELETE FROM {table} WHERE {column} IN ({placeholders})", values)

    def _channel_member_count_locked(self, channel_id: str) -> int:
        row = self._conn.execute("SELECT COUNT(*) AS count FROM channel_members WHERE channel_id = ?", (channel_id,)).fetchone()
        return int(row["count"])

    def _ensure_channel_locked(self, channel_id: str) -> None:
        self._conn.execute(
            "INSERT INTO channels (channel_id, created_at) VALUES (?, ?) ON CONFLICT(channel_id) DO NOTHING",
            (channel_id, utc_now().isoformat()),
        )

    def _insert_channel_member_locked(self, channel_id: str, alias: str, *, role: str = "member") -> None:
        self._ensure_channel_locked(channel_id)
        self._conn.execute(
            """
            INSERT INTO channel_members (channel_id, alias, created_at, role)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(channel_id, alias) DO NOTHING
            """,
            (channel_id, alias, utc_now().isoformat(), role),
        )

    def _issue_token_locked(self, channel_id: str, *, alias: str | None = None) -> TokenResponse:
        self._ensure_channel_locked(channel_id)
        created_at = utc_now()
        expires_at = created_at + TOKEN_TTL
        member_token = secrets.token_urlsafe(32)
        self._conn.execute(
            """
            INSERT INTO member_tokens (token_hash, channel_id, created_at, expires_at, revoked_at, alias)
            VALUES (?, ?, ?, ?, NULL, ?)
            """,
            (self._hash_token(member_token), channel_id, created_at.isoformat(), expires_at.isoformat(), alias),
        )
        return TokenResponse(channel_id=channel_id, member_token=member_token, expires_at=expires_at, alias=alias)

    def _append_join_request_events_locked(self, channel_id: str, request_id: str, alias: str, requested_at: datetime) -> None:
        rows = self._conn.execute("SELECT client_id FROM clients WHERE channel_id = ? ORDER BY client_id ASC", (channel_id,)).fetchall()
        for row in rows:
            self._append_client_control_event_locked(
                str(row["client_id"]),
                "join.request",
                {"request_id": request_id, "alias": alias, "channel_id": channel_id, "requested_at": requested_at.isoformat()},
            )

    def command_output_path(self, command_id: str, stream: str) -> Path:
        return self.commands_root / f"{command_id}.{stream}"

    def _notify_update(self) -> None:
        with self._updates:
            self._updates.notify_all()

    def _append_client_control_event_locked(self, client_id: str, kind: str, payload: dict) -> ClientControlEvent:
        now = utc_now()
        seq_row = self._conn.execute(
            "SELECT COALESCE(MAX(seq), 0) AS max_seq FROM client_control_events WHERE client_id = ?",
            (client_id,),
        ).fetchone()
        seq = int(seq_row["max_seq"]) + 1
        self._conn.execute(
            "INSERT INTO client_control_events (client_id, seq, kind, payload_json, created_at) VALUES (?, ?, ?, ?, ?)",
            (client_id, seq, kind, json.dumps(payload), now.isoformat()),
        )
        return ClientControlEvent(event_id=seq, client_id=client_id, kind=kind, payload=payload, created_at=now)

    def _append_command_event_locked(self, command_id: str, kind: str, payload: dict) -> EventRecord:
        return self._append_stream_event_locked("command_events", "command_id", command_id, kind, payload)

    def _append_shell_event_locked(self, session_id: str, kind: str, payload: dict) -> EventRecord:
        return self._append_stream_event_locked("shell_stream_events", "session_id", session_id, kind, payload)

    def _append_file_event_locked(self, transfer_id: str, kind: str, payload: dict) -> EventRecord:
        return self._append_stream_event_locked("file_events", "transfer_id", transfer_id, kind, payload)

    def _append_stream_event_locked(self, table: str, key_column: str, key_value: str, kind: str, payload: dict) -> EventRecord:
        now = utc_now()
        seq_row = self._conn.execute(
            f"SELECT COALESCE(MAX(seq), 0) AS max_seq FROM {table} WHERE {key_column} = ?",
            (key_value,),
        ).fetchone()
        seq = int(seq_row["max_seq"]) + 1
        self._conn.execute(
            f"INSERT INTO {table} ({key_column}, seq, kind, payload_json, created_at) VALUES (?, ?, ?, ?, ?)",
            (key_value, seq, kind, json.dumps(payload), now.isoformat()),
        )
        return EventRecord(event_id=seq, kind=kind, payload=payload, created_at=now)

    def _list_events(self, table: str, key_column: str, key_value: str, after_seq: int) -> list[EventRecord]:
        with self._lock:
            rows = self._conn.execute(
                f"SELECT seq, kind, payload_json, created_at FROM {table} WHERE {key_column} = ? AND seq > ? ORDER BY seq ASC",
                (key_value, after_seq),
            ).fetchall()
        return [self._row_to_event(dict(row)) for row in rows]

    @staticmethod
    def _read_from_offset(path: Path, offset: int) -> str:
        if not path.exists():
            return ""
        with path.open("r", encoding="utf-8") as handle:
            handle.seek(offset)
            return handle.read()

    @staticmethod
    def _hash_token(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    @staticmethod
    def channel_id_for_name(channel: str) -> str:
        digest = hashlib.sha256(channel.encode("utf-8")).hexdigest()[:16]
        return f"channel-{digest}"

    @staticmethod
    def _command_values(record: CommandRecord) -> tuple:
        return (
            record.command_id,
            record.client_id,
            record.channel_id,
            json.dumps(record.argv),
            json.dumps(record.env_patch),
            record.timeout_sec,
            record.working_dir,
            record.claim_timeout_sec,
            record.status.value,
            record.created_at.isoformat(),
            record.started_at.isoformat() if record.started_at else None,
            record.finished_at.isoformat() if record.finished_at else None,
            record.heartbeat_at.isoformat() if record.heartbeat_at else None,
            record.cancel_requested_at.isoformat() if record.cancel_requested_at else None,
            record.exit_code,
            record.failure_code,
            record.stdout_path,
            record.stderr_path,
        )

    @staticmethod
    def _file_transfer_values(record: FileTransferRecord) -> tuple:
        return (
            record.transfer_id,
            record.client_id,
            record.channel_id,
            record.direction,
            record.remote_path,
            record.size,
            record.max_bytes,
            record.status.value,
            record.created_at.isoformat(),
            record.started_at.isoformat() if record.started_at else None,
            record.finished_at.isoformat() if record.finished_at else None,
            record.failure_code,
            record.data_b64,
        )

    @staticmethod
    def _shell_values(record: ShellSessionRecord) -> tuple:
        return (
            record.session_id,
            record.client_id,
            record.channel_id,
            record.cwd_root,
            record.status.value,
            record.created_at.isoformat(),
            record.started_at.isoformat() if record.started_at else None,
            record.finished_at.isoformat() if record.finished_at else None,
            record.heartbeat_at.isoformat() if record.heartbeat_at else None,
            record.close_requested_at.isoformat() if record.close_requested_at else None,
            record.exit_code,
            record.failure_code,
        )

    @staticmethod
    def _row_to_event(row: dict) -> EventRecord:
        return EventRecord(event_id=int(row["seq"]), kind=str(row["kind"]), payload=json.loads(str(row["payload_json"])), created_at=_parse_dt(row["created_at"]))

    @staticmethod
    def _row_to_channel(row: dict) -> ChannelRecord:
        return ChannelRecord(channel_id=row["channel_id"], created_at=_parse_dt(row["created_at"]))

    @staticmethod
    def _row_to_client(row: dict) -> ClientRecord:
        stream_connected = row.get("stream_connected")
        return ClientRecord(
            client_id=row["client_id"],
            channel_id=row["channel_id"],
            created_at=_parse_dt(row["created_at"]),
            last_seen_at=_parse_dt(row["last_seen_at"]),
            stream_connected=None if stream_connected is None else bool(stream_connected),
        )

    @staticmethod
    def _row_to_join_approval(row: dict) -> JoinApprovalRecord:
        return JoinApprovalRecord(
            request_id=row["request_id"],
            channel_id=row["channel_id"],
            alias=row["alias"],
            status=JoinRequestStatus(row["status"]),
            requested_at=_parse_dt(row["requested_at"]),
            approved_at=_parse_dt(row["approved_at"]),
            approved_by=row["approved_by"],
            rejected_at=_parse_dt(row["rejected_at"]),
            rejected_by=row["rejected_by"],
        )

    @staticmethod
    def _row_to_file_transfer(row: dict) -> FileTransferRecord:
        return FileTransferRecord(
            transfer_id=row["transfer_id"],
            client_id=row["client_id"],
            channel_id=row["channel_id"],
            direction=row["direction"],
            remote_path=row["remote_path"],
            size=int(row["size"]),
            max_bytes=int(row["max_bytes"]),
            status=FileTransferStatus(row["status"]),
            created_at=_parse_dt(row["created_at"]),
            started_at=_parse_dt(row["started_at"]),
            finished_at=_parse_dt(row["finished_at"]),
            failure_code=row["failure_code"],
            data_b64=row["data_b64"],
        )

    @staticmethod
    def _row_to_command(row: dict) -> CommandRecord:
        return CommandRecord(
            command_id=row["command_id"],
            client_id=row["client_id"],
            channel_id=row["channel_id"],
            argv=json.loads(row["argv"]),
            env_patch=json.loads(row["env_patch"] or "{}"),
            timeout_sec=int(row["timeout_sec"]),
            working_dir=row["working_dir"],
            claim_timeout_sec=int(row["claim_timeout_sec"]) if row.get("claim_timeout_sec") is not None else None,
            status=CommandStatus(row["status"]),
            created_at=_parse_dt(row["created_at"]),
            started_at=_parse_dt(row["started_at"]),
            finished_at=_parse_dt(row["finished_at"]),
            heartbeat_at=_parse_dt(row["heartbeat_at"]),
            cancel_requested_at=_parse_dt(row["cancel_requested_at"]),
            exit_code=row["exit_code"],
            failure_code=row["failure_code"],
            stdout_path=row["stdout_path"],
            stderr_path=row["stderr_path"],
        )

    @staticmethod
    def _row_to_shell(row: dict) -> ShellSessionRecord:
        return ShellSessionRecord(
            session_id=row["session_id"],
            client_id=row["client_id"],
            channel_id=row["channel_id"],
            cwd_root=row["cwd_root"],
            status=ShellSessionStatus(row["status"]),
            created_at=_parse_dt(row["created_at"]),
            started_at=_parse_dt(row["started_at"]),
            finished_at=_parse_dt(row["finished_at"]),
            heartbeat_at=_parse_dt(row["heartbeat_at"]),
            close_requested_at=_parse_dt(row["close_requested_at"]),
            exit_code=row["exit_code"],
            failure_code=row["failure_code"],
        )


def _parse_dt(value: str | None):
    if value is None:
        return None
    return datetime.fromisoformat(value)
