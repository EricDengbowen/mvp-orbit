from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from datetime import datetime

from mvp_orbit.client.runtime import ClientRuntime
from mvp_orbit.client.service import ClientService, StreamFailureLimitError
from mvp_orbit.core.logging import configure_logging, log_kv
from mvp_orbit.core.models import utc_now


def _required(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"missing env var: {name}")
    return value


def _load_renewed_credentials(current_token: str) -> tuple[str, datetime] | None:
    """Return a (token, expires_at) pair from the saved config when it holds a
    different, still-valid token than the one this process started with —
    the auto-renewal thread updates the config while we run, and swapping to
    it beats dying at the old token's expiry."""
    try:
        from mvp_orbit.config import load_config

        _, config = load_config()
    except Exception:
        return None
    token = config.auth.member_token
    expires = config.auth.expires_at
    if not token or expires is None or token == current_token or expires <= utc_now():
        return None
    return token, expires


def main() -> None:
    client_id = _required("ORBIT_CLIENT_ID")
    hub_url = _required("ORBIT_HUB_URL")
    member_token = _required("ORBIT_MEMBER_TOKEN")
    expires_at = datetime.fromisoformat(_required("ORBIT_TOKEN_EXPIRES_AT"))
    if expires_at <= utc_now():
        renewed = _load_renewed_credentials(member_token)
        if renewed is None:
            raise RuntimeError("member token expired; run `orbit join` again")
        member_token, expires_at = renewed
    configure_logging("client")
    workspace_root = os.getenv("ORBIT_WORKSPACE_ROOT")
    if workspace_root:
        root = Path(workspace_root).expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        os.chdir(root)
    base_workspace = Path.cwd()
    logger = logging.getLogger(__name__)
    log_kv(logger, logging.INFO, "client.start", client_id=client_id, hub_url=hub_url, workspace=base_workspace)

    runtime = ClientRuntime(
        client_id=client_id,
        base_workspace=base_workspace,
        command_output_chunk_bytes=int(os.getenv("ORBIT_COMMAND_OUTPUT_CHUNK_BYTES", "4096")),
        command_output_flush_interval_sec=float(os.getenv("ORBIT_COMMAND_OUTPUT_FLUSH_SEC", "0.1")),
    )
    while True:
        service = ClientService(
            client_id=client_id,
            hub_url=hub_url,
            runtime=runtime,
            member_token=member_token,
            heartbeat_interval_sec=float(os.getenv("ORBIT_HEARTBEAT_SEC", "15")),
            max_stream_failures=int(os.getenv("ORBIT_MAX_STREAM_FAILURES", "10")),
        )
        try:
            service.run_forever()
            return
        except StreamFailureLimitError as exc:
            # Network-level give-up: a fresh token would not help; exit so the
            # supervisor restarts us with a fresh environment.
            print(f"[orbit] client stopped: {exc}", file=sys.stderr, flush=True)
            raise SystemExit(3) from exc
        except RuntimeError as exc:
            # Token-related exit: if auto-renewal left a newer valid token in
            # the config, swap to it and reconnect instead of dying.
            renewed = _load_renewed_credentials(member_token)
            if renewed is not None:
                member_token, _ = renewed
                log_kv(logger, logging.INFO, "client.credentials_refreshed", client_id=client_id, action="reconnecting with the renewed token")
                continue
            print(f"[orbit] client stopped: {exc}", file=sys.stderr, flush=True)
            raise SystemExit(3) from exc


if __name__ == "__main__":
    main()
