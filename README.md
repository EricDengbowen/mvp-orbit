<p align="center">
  <img alt="mvp-orbit icon" src="./assets/icon.svg" width="96" height="96">
</p>

<h1 align="center">MVP Orbit</h1>

<p align="center">by MVP Lab.</p>

<p align="center">
  <a href="https://github.com/mvp-ai-lab/mvp-orbit/releases"><img alt="GitHub release" src="https://img.shields.io/github/v/release/mvp-ai-lab/mvp-orbit?style=flat-square"></a>
  <img alt="Python 3.11+" src="https://img.shields.io/badge/python-%3E%3D3.11-3776AB?style=flat-square&logo=python&logoColor=white">
  <img alt="CLI orbit" src="https://img.shields.io/badge/cli-orbit-111827?style=flat-square">
  <img alt="Transport HTTP/SSE" src="https://img.shields.io/badge/transport-HTTP%20%2B%20SSE-0F766E?style=flat-square">
  <img alt="Modes exec shell files" src="https://img.shields.io/badge/modes-exec%20%7C%20shell%20%7C%20files-7C3AED?style=flat-square">
</p>

## What Orbit Is

`mvp-orbit` is a small HTTP-only peer command channel.

Run one control `host`, let multiple `client` machines join the same channel, approve new members from any existing client, then run commands, open shells, and move files between approved peers. Clients only need to reach the host; they do not need direct network access to each other.

## Features

- Simple join flow: `host URL` + local `alias` + `channel` name.
- First client in a channel is accepted automatically and becomes the channel admin.
- Later clients require approval from any existing channel member.
- Foreground `orbit join` clients can prompt directly when a new client asks to join (the prompt never blocks command delivery).
- Peer command mode: `orbit exec <peer> -- <command>` waits for output and exit status.
- Interactive shell mode: `orbit sh <peer>` opens a live shell on the target client.
- File transfer mode: `orbit put` and `orbit get`, with a default `1 MiB` limit.
- HTTP/SSE transport through the host, with no direct client-to-client networking.
- Fail-fast everywhere: unclaimed work times out (`unclaimed`, exit 125), bad binaries fail immediately instead of hanging, and clients exit non-zero after repeated stream failures so a supervisor can take over.
- Built-in supervision and diagnosis: `orbit join --daemon`, `orbit status` (local), `orbit doctor <peer>` (remote).
- Member management: `orbit members`, `orbit leave`, `orbit remove <alias>`, `orbit transfer-admin <alias>`.
- Automatic empty-channel cleanup on the host (default TTL: 7 days).

## Quick Start

### 1. Install or Run

From a checkout:

```bash
uv run orbit --help
```

Install the current GitHub release wheel as a tool:

```bash
uv tool install https://github.com/mvp-ai-lab/mvp-orbit/releases/download/v0.7.0/mvp_orbit-0.7.0-py3-none-any.whl
```

### 2. Start the Host

```bash
orbit host
```

By default the host listens on `127.0.0.1:8080`. To accept clients from other machines:

```bash
ORBIT_HUB_HOST=0.0.0.0 orbit host
```

### 3. Join the First Client

```bash
orbit join --host http://HOST:8080 --alias client-a --channel team-a
```

`orbit join` stays in the foreground. Once joined, this process receives commands, shells, file requests, and join approvals.

### 4. Join Another Client

On another machine:

```bash
orbit join --host http://HOST:8080 --alias client-b --channel team-a
```

The first client sees an approval prompt:

```text
[orbit] new client join request
  alias: client-b
  channel: channel-...
  request: join-...
[orbit] approve this client? [y/N]:
```

If no interactive client is available, approve manually from any existing member:

```bash
orbit join-requests
orbit approve <REQUEST_ID>
```

Reject a request with:

```bash
orbit reject <REQUEST_ID>
```

### 5. Use the Channel

List peers:

```bash
orbit peers
```

Run one command and wait for the result:

```bash
orbit exec client-b -- uname -a
orbit exec client-b --shell "cd /tmp && pwd && ls -la"
```

Open an interactive shell:

```bash
orbit sh client-b
```

Send a file to a peer:

```bash
orbit put client-b ./local.txt inbox/local.txt
```

Download a file from a peer:

```bash
orbit get client-b inbox/local.txt ./downloaded.txt
```

Raise the default `1 MiB` file limit only when needed:

```bash
orbit put --max-bytes 10485760 client-b ./model.bin models/model.bin
orbit get --max-bytes 10485760 client-b models/model.bin ./model.bin
```

## How It Works

```mermaid
flowchart LR
    H["control host\nHTTP + SSE relay"]
    A["client-a"]
    B["client-b"]
    C["client-c"]

    A <-->|"join approval\ncommands / shells / files"| H
    B <-->|"join approval\ncommands / shells / files"| H
    C <-->|"join approval\ncommands / shells / files"| H
```

The host stores channel state in SQLite and relays events. Each client keeps a foreground SSE connection to the host and posts command, shell, and file results back over HTTP.

## CLI Reference

The public command surface is intentionally small:

```bash
orbit host
orbit join
orbit join-requests
orbit approve <REQUEST_ID>
orbit reject <REQUEST_ID>
orbit peers
orbit exec <peer> -- <command>
orbit sh <peer>
orbit put <peer> <local> <remote>
orbit get <peer> <remote> <local>
orbit status                   # local client health, no token needed
orbit doctor <peer>            # remote diagnosis with a probe command
orbit members                  # list members, roles, liveness
orbit leave                    # leave the channel, revoke own credentials
orbit remove <alias>           # admin: evict a member, revoke its credentials
orbit transfer-admin <alias>   # admin: hand over the admin role
```

Useful `join` options:

```bash
orbit join --no-start   # save config without starting the client loop
orbit join --no-wait    # submit a join request and exit immediately
orbit join --daemon     # run the client loop as a supervised background daemon
                        # (auto-restart with backoff, pidfile + log under
                        #  ~/.local/state/mvp-orbit/)
```

Useful `exec` options (accepted before or after the peer name):

```bash
orbit exec client-b --timeout-sec 60 -- long-task     # kill the command after 60s
orbit exec client-b --claim-timeout 10 -- quick-check  # fail fast if the peer is offline
orbit exec client-b --shell 'echo hi | wc -c'          # run through /bin/sh -lc
```

Exit codes: the remote command's own exit code, `124` for a command timeout, `125` when no peer claimed the command in time, `130` when canceled, and `3` when the client loop gives up after repeated stream failures.

Commands run inside the target client's workspace. `--working-dir` must stay inside that workspace. Relative remote file paths are resolved under the target client's workspace; absolute remote paths are allowed and should be used carefully.

## Security Model

Channel membership is the trust boundary.

- The first client creates the channel, receives a member token, and becomes the channel **admin**.
- Later clients cannot join until an existing member approves the join request. This includes re-enrollment of an existing alias (e.g. after a token expired): `/api/join` is unauthenticated, so handing out tokens for a claimed alias without approval would let anyone mint a member's credentials.
- A member token grants access to that channel until it expires or is revoked.
- Any approved member can execute commands on any other connected member.
- Only admins can `remove` members or change roles; anyone can `leave`. Removing a member (or leaving) revokes that alias's tokens immediately. The last admin must `transfer-admin` before leaving; when the last member leaves, the channel is deleted.

This is not a sandbox. Only approve clients and run commands in channels where every member is trusted.

## Reliability Behavior

- The client exits non-zero (code 3) after `ORBIT_MAX_STREAM_FAILURES` consecutive stream failures (default 10, `0` = retry forever) with exponential backoff in between — a supervisor (`orbit join --daemon`, systemd, a watchdog script) then restarts it with a fresh environment.
- Heartbeats report event-stream health separately: `orbit peers` / `orbit members` show `stream_connected`, so a machine whose process is alive but deaf is visible instead of silently queueing work.
- The host fails QUEUED work nobody claims within `ORBIT_CLAIM_TIMEOUT_SEC` (default 30s, per-command override with `orbit exec --claim-timeout N`) as `unclaimed`, so callers get a fast, diagnosable failure instead of an infinite hang.
- `orbit status` answers "is my own client healthy?" locally; `orbit doctor <peer>` distinguishes process-dead / deaf-client / healthy for a remote machine.

## Configuration

The default config file is:

```text
~/.config/mvp-orbit/config.toml
```

`orbit join` writes the host URL, local client alias, member token, and token expiry. Non-join commands read this file automatically. You can override values with CLI flags such as `--hub-url`, `--member-token`, and `--token-expires-at`.

Useful runtime environment variables:

```bash
ORBIT_CONFIG=~/.config/mvp-orbit/config.toml
ORBIT_WORKSPACE_ROOT=/path/to/workspace   # pinned automatically at first join
ORBIT_HEARTBEAT_SEC=15
ORBIT_MAX_STREAM_FAILURES=10  # 0 = retry forever
ORBIT_STATE_DIR=~/.local/state/mvp-orbit  # status file, daemon log + pidfile
ORBIT_LOG_LEVEL=INFO      # DEBUG, INFO, WARNING, ERROR
NO_COLOR=1               # disable ANSI colors
```

Host environment variables:

```bash
ORBIT_HUB_HOST=127.0.0.1
ORBIT_HUB_PORT=8080
ORBIT_HUB_DB=./.orbit-hub/hub.sqlite3
ORBIT_OBJECT_ROOT=./.orbit-hub/objects
ORBIT_CLAIM_TIMEOUT_SEC=30        # fail queued work nobody claims (0 = disable)
ORBIT_GRACEFUL_SHUTDOWN_SEC=10    # bound shutdown draining of open SSE streams
ORBIT_ACCESS_LOG=0        # set to 1 to enable uvicorn HTTP access logs
```

## Empty Channel Cleanup

The host automatically removes channels that have no online clients. Clients send heartbeat events while `orbit join` is running. A channel is considered empty when no client has been seen within `ORBIT_CLIENT_OFFLINE_SEC`, and it is deleted after `ORBIT_CHANNEL_EMPTY_TTL_SEC` of no activity.

Defaults:

```bash
ORBIT_CHANNEL_CLEANUP_ENABLED=1
ORBIT_CLIENT_OFFLINE_SEC=90
ORBIT_CHANNEL_EMPTY_TTL_SEC=604800   # 7 days
ORBIT_CHANNEL_CLEANUP_INTERVAL_SEC=60
```

Deleting a channel removes its approved members, pending join requests, stale client records, tokens, command history, shell history, and file-transfer history for that channel.

## Logging

Runtime logs use a compact structured line format:

```text
[15:52:34] INFO    client │ client.runtime     │ command.start client_id=client-a argv="python3 -V"
```

The message part uses `event key=value` so it remains easy to search and parse.

## Docker Host

The Dockerfile runs the host:

```bash
docker build -t mvp-orbit .
docker run --rm -p 8080:8080 -v orbit-data:/var/lib/orbit mvp-orbit
```

The image sets `ORBIT_HUB_HOST=0.0.0.0` and stores state under `/var/lib/orbit`.

## Network Model

Only these connections are required:

- each client can reach the control host over HTTP or HTTPS
- the host does not need to initiate connections back to clients
- clients do not need direct connectivity to each other

Realtime delivery uses SSE from host to client and HTTP POST from client to host.
