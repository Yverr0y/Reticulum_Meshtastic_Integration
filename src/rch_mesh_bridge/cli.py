from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path

from .config import BridgeConfigError, RuntimeConfig, infer_runtime_paths, load_config
from .service import BridgeService, configure_logging
from .status import RuntimeStatusStore


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rch-mesh-bridge",
        description="Meshtastic to RCH bridge",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    start = subparsers.add_parser("start", help="Start the bridge")
    start.add_argument("--config", default="config.ini", help="Path to config.ini")
    start.add_argument(
        "--foreground",
        action="store_true",
        help="Run in foreground (default is daemon mode)",
    )

    stop = subparsers.add_parser("stop", help="Stop the bridge")
    stop.add_argument("--config", default="config.ini", help="Path to config.ini")

    status = subparsers.add_parser("status", help="Bridge status")
    status.add_argument("--config", default="config.ini", help="Path to config.ini")
    status.add_argument("--json", action="store_true", help="Output JSON only")

    return parser


def _is_pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        import ctypes

        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        handle = ctypes.windll.kernel32.OpenProcess(  # type: ignore[attr-defined]
            PROCESS_QUERY_LIMITED_INFORMATION, 0, pid
        )
        if handle:
            ctypes.windll.kernel32.CloseHandle(handle)  # type: ignore[attr-defined]
            return True
        return False

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _read_pid(pid_file: Path) -> int | None:
    if not pid_file.exists():
        return None
    try:
        value = pid_file.read_text(encoding="utf-8").strip()
        return int(value)
    except (OSError, ValueError):
        return None


def _resolve_runtime(config_path: Path) -> RuntimeConfig:
    try:
        config = load_config(config_path)
    except BridgeConfigError:
        return infer_runtime_paths(config_path)
    return config.runtime


def _remove_runtime_files(runtime: RuntimeConfig) -> None:
    for target in (runtime.pid_file, runtime.status_file):
        try:
            target.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            pass


@contextmanager
def _pid_file_lock(pid_file: Path):
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    existing = _read_pid(pid_file)
    if existing is not None:
        if _is_pid_running(existing):
            raise RuntimeError(f"Bridge is already running with PID {existing}")
        try:
            pid_file.unlink()
        except OSError:
            pass

    current_pid = os.getpid()
    pid_file.write_text(str(current_pid), encoding="utf-8")
    try:
        yield current_pid
    finally:
        try:
            if _read_pid(pid_file) == current_pid:
                pid_file.unlink()
        except OSError:
            pass


def _run_foreground(config_path: Path) -> int:
    config = load_config(config_path)
    configure_logging(config.general.log_level)
    stop_event = asyncio.Event()

    def _handle_signal(signum, frame) -> None:
        del signum, frame
        stop_event.set()

    previous_sigint = signal.getsignal(signal.SIGINT)
    previous_sigterm = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    service = BridgeService(config)
    threading_stop_event = _AsyncioEventBridge(stop_event)

    try:
        with _pid_file_lock(config.runtime.pid_file):
            asyncio.run(service.run(threading_stop_event))
    finally:
        signal.signal(signal.SIGINT, previous_sigint)
        signal.signal(signal.SIGTERM, previous_sigterm)

    return 0


class _AsyncioEventBridge:
    """Bridge an asyncio.Event into the threading.Event-like interface used by service."""

    def __init__(self, event: asyncio.Event) -> None:
        self._event = event

    def is_set(self) -> bool:
        return self._event.is_set()

    def set(self) -> None:
        self._event.set()


def _start_daemon(config_path: Path) -> int:
    config = load_config(config_path)
    runtime = config.runtime
    running_pid = _read_pid(runtime.pid_file)
    if running_pid and _is_pid_running(running_pid):
        print(f"Bridge already running (PID {running_pid})")
        return 1

    cmd = [
        sys.executable,
        "-m",
        "rch_mesh_bridge.cli",
        "start",
        "--foreground",
        "--config",
        str(config_path),
    ]
    kwargs = {
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "cwd": str(config_path.parent),
    }

    if os.name == "nt":
        flags = 0
        if hasattr(subprocess, "DETACHED_PROCESS"):
            flags |= subprocess.DETACHED_PROCESS
        if hasattr(subprocess, "CREATE_NEW_PROCESS_GROUP"):
            flags |= subprocess.CREATE_NEW_PROCESS_GROUP
        kwargs["creationflags"] = flags
    else:
        kwargs["start_new_session"] = True

    process = subprocess.Popen(cmd, **kwargs)
    deadline = time.time() + 10.0
    while time.time() < deadline:
        if runtime.pid_file.exists():
            print(f"Bridge started with PID {_read_pid(runtime.pid_file)}")
            return 0
        if process.poll() is not None:
            print("Bridge failed to start")
            return process.returncode or 1
        time.sleep(0.1)

    print("Bridge start timed out waiting for PID file")
    return 1


def _handle_start(args: argparse.Namespace) -> int:
    config_path = Path(args.config).expanduser().resolve()
    if args.foreground:
        return _run_foreground(config_path)
    return _start_daemon(config_path)


def _handle_stop(args: argparse.Namespace) -> int:
    config_path = Path(args.config).expanduser().resolve()
    runtime = _resolve_runtime(config_path)
    pid = _read_pid(runtime.pid_file)
    if pid is None:
        _remove_runtime_files(runtime)
        print("Bridge is not running")
        return 0

    if not _is_pid_running(pid):
        _remove_runtime_files(runtime)
        print("Bridge is not running")
        return 0

    try:
        os.kill(pid, signal.SIGTERM)
    except OSError as exc:
        print(f"Failed to stop PID {pid}: {exc!s}")
        return 1

    deadline = time.time() + 10.0
    while time.time() < deadline:
        if not _is_pid_running(pid):
            break
        time.sleep(0.1)

    if _is_pid_running(pid):
        print(f"Timed out waiting for PID {pid} to stop")
        return 1

    _remove_runtime_files(runtime)
    print("Bridge stopped")
    return 0


def _handle_status(args: argparse.Namespace) -> int:
    config_path = Path(args.config).expanduser().resolve()
    runtime = _resolve_runtime(config_path)
    pid = _read_pid(runtime.pid_file)
    running = bool(pid and _is_pid_running(pid))
    snapshot = RuntimeStatusStore.read_snapshot_file(runtime.status_file)

    if running:
        connected = bool(snapshot.get("meshtastic_connected", False))
        state = "running" if connected else "degraded"
        observed_nodes = int(snapshot.get("observed_nodes", 0))
        last_packet = snapshot.get("last_packet")
    else:
        state = "stopped"
        connected = False
        observed_nodes = 0
        last_packet = None

    payload = {
        "state": state,
        "meshtastic_connected": connected,
        "observed_nodes": observed_nodes,
        "last_packet": last_packet,
    }

    if args.json:
        print(json.dumps(payload))
        return 0

    print(f"state: {payload['state']}")
    print(f"meshtastic_connected: {payload['meshtastic_connected']}")
    print(f"observed_nodes: {payload['observed_nodes']}")
    print(f"last_packet: {payload['last_packet']}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "start":
            return _handle_start(args)
        if args.command == "stop":
            return _handle_stop(args)
        if args.command == "status":
            return _handle_status(args)
    except BridgeConfigError as exc:
        print(f"Configuration error: {exc!s}")
        return 2
    except RuntimeError as exc:
        print(str(exc))
        return 1

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
