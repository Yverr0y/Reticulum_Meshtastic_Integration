from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path


def test_cli_start_status_stop(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    config_path = tmp_path / "config.ini"
    config_path.write_text(
        """
[meshtastic]
host = 127.0.0.1
port = 9
channel = 0

[rch]
rest_url = http://127.0.0.1:9
api_token = test-token
identity = default
auth_mode = bearer
timeout_seconds = 0.5

[runtime]
pid_file = .runtime/bridge.pid
status_file = .runtime/status.json
reconnect_max_seconds = 1

[general]
log_level = WARNING
""".strip(),
        encoding="utf-8",
    )

    runtime_dir = tmp_path / ".runtime"
    pid_file = runtime_dir / "bridge.pid"
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_root / "src") + os.pathsep + env.get("PYTHONPATH", "")

    start_result = subprocess.run(
        [
            sys.executable,
            "-m",
            "rch_mesh_bridge.cli",
            "start",
            "--config",
            str(config_path),
        ],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
    )
    assert start_result.returncode == 0, start_result.stderr

    deadline = time.time() + 10.0
    while time.time() < deadline and not pid_file.exists():
        time.sleep(0.1)

    assert pid_file.exists(), "pid file not created by start command"

    try:
        status_result = subprocess.run(
            [
                sys.executable,
                "-m",
                "rch_mesh_bridge.cli",
                "status",
                "--json",
                "--config",
                str(config_path),
            ],
            cwd=repo_root,
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )
        payload = json.loads(status_result.stdout.strip())
        assert set(payload.keys()) == {
            "state",
            "meshtastic_connected",
            "observed_nodes",
            "last_packet",
        }
    finally:
        stop_result = subprocess.run(
            [
                sys.executable,
                "-m",
                "rch_mesh_bridge.cli",
                "stop",
                "--config",
                str(config_path),
            ],
            cwd=repo_root,
            env=env,
            capture_output=True,
            text=True,
        )
        assert stop_result.returncode == 0
        assert not pid_file.exists()
