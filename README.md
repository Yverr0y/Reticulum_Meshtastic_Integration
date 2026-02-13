# Reticulum Meshtastic Integration

`Reticulum_Meshtastic_Integration` is a standalone Python 3.12 service that ingests Meshtastic packets over WiFi, maps nodes to RCH objects (markers), and forwards telemetry plus broadcast chat into Reticulum Community Hub (RCH) over REST.

## Scope

- In:
  - Meshtastic TCP/WiFi ingest (`host`, `port`, `channel`)
  - Position forwarding to RCH marker APIs
  - Broadcast chat forwarding to RCH topic/message APIs
  - Single RCH identity/API token
  - `start`, `stop`, `status` CLI
  - Stateless operation (in-memory node/topic cache only)
  - Auto-reconnect with exponential backoff
- Out:
  - Bluetooth/serial transports
  - Direct message bridging
  - Reticulum to Meshtastic reverse flow
  - Persistent telemetry/chat history
  - Rate limiting

## Requirements

- Python `3.12+`
- Network access to:
  - Meshtastic TCP endpoint (default `4403`)
  - RCH REST endpoint
- Reticulum Community Hub (RCH) is required for this integration and must be running before the bridge starts.

## Install

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
```

On Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
```

## Configuration

Use `config.ini` and update required fields:

- `[meshtastic].host`
- `[rch].rest_url` (optional; defaults to `http://localhost:8000`)
- `[rch].auth_mode` (optional; default is unauthenticated `none`)
- `[rch].api_token` (required only for `bearer` or `x_api_key`)

Runtime paths (`pid_file`, `status_file`) are resolved relative to the config file directory when not absolute.

### Meshtastic Device Role Guidance

- Do **not** use Meshtastic device role `TAK` for this bridge.
- Recommended roles are `TRACKER` (`TAK_TRACKER`) or `CLIENT`.
- `TRACKER` and `CLIENT` both work correctly for regular position flow and ATAK plugin payload ingestion.

## CLI

Important: start and verify RCH first, then start `rch-mesh-bridge`.

### Start (daemon mode by default)

```bash
rch-mesh-bridge start --config ./config.ini
```

### Start in foreground

```bash
rch-mesh-bridge start --foreground --config ./config.ini
```

### Stop

```bash
rch-mesh-bridge stop --config ./config.ini
```

### Status

```bash
rch-mesh-bridge status --json --config ./config.ini
```

Example:

```json
{
  "state": "running",
  "meshtastic_connected": true,
  "observed_nodes": 4,
  "last_packet": "2026-02-12T12:30:00Z"
}
```

## RCH REST Mapping

- Telemetry/object:
  - `POST /api/markers`
  - `PATCH /api/markers/{object_destination_hash}/position`
- Chat/topic:
  - `GET /Topic`
  - `POST /Topic`
  - `POST /Message`

## systemd

Example unit: `infra/systemd/rch-mesh-bridge.service`

```bash
sudo cp infra/systemd/rch-mesh-bridge.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now rch-mesh-bridge
```

## Development

Run tests:

```bash
pytest -q
```

## Troubleshooting

- `state=degraded`, `meshtastic_connected=false`
  - Check Meshtastic host/port reachability.
  - Confirm device exposes TCP API on the expected network.
- Marker creation fails
  - Validate RCH auth mode (`none`, `bearer`, `x_api_key`).
  - Verify token has access to `/api/markers`.
  - The bridge auto-retries marker creation with a supported symbol from `/api/markers/symbols` when RCH returns `422` for marker type/symbol.
  - Standard Meshtastic marker defaults to `map-marker-account`; if `marker_type` is `auto` (or left legacy `meshtastic_node`), the bridge sends marker type equal to the selected symbol.
- Chat not visible
  - Check topic permissions and `POST /Message` authorization.
  - Confirm `topic_path_template` resolves as expected.
- Reconnect loop noisy
  - Increase `[runtime].reconnect_max_seconds`.
  - Set `[general].log_level = WARNING` for reduced logging.
- No/limited updates when radio role is `TAK`
  - Change device role to `TRACKER` (`TAK_TRACKER`) or `CLIENT`.
