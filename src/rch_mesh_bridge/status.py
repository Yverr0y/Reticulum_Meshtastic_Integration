from __future__ import annotations

import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from .models import BridgeStatusSnapshot


class RuntimeStatusStore:
    def __init__(self, status_file: Path) -> None:
        self._status_file = status_file
        self._lock = threading.Lock()
        self._meshtastic_connected = False
        self._observed_nodes = 0
        self._last_packet: datetime | None = None

    @property
    def status_file(self) -> Path:
        return self._status_file

    def set_meshtastic_connected(self, value: bool) -> None:
        with self._lock:
            self._meshtastic_connected = bool(value)

    def set_observed_nodes(self, value: int) -> None:
        with self._lock:
            self._observed_nodes = max(0, int(value))

    def mark_packet(self, timestamp: datetime) -> None:
        with self._lock:
            self._last_packet = timestamp

    def is_connected(self) -> bool:
        with self._lock:
            return self._meshtastic_connected

    def snapshot(self, state: str) -> BridgeStatusSnapshot:
        with self._lock:
            return BridgeStatusSnapshot(
                state=state,
                meshtastic_connected=self._meshtastic_connected,
                observed_nodes=self._observed_nodes,
                last_packet=self._last_packet,
            )

    def write_snapshot(self, state: str) -> BridgeStatusSnapshot:
        snapshot = self.snapshot(state=state)
        self._status_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._status_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(snapshot.to_dict(), indent=2), encoding="utf-8")
        tmp.replace(self._status_file)
        return snapshot

    @staticmethod
    def read_snapshot_file(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}

