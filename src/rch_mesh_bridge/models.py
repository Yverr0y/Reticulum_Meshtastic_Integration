from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


def to_rfc3339(timestamp: datetime | None) -> str | None:
    if timestamp is None:
        return None

    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    return timestamp.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class MeshtasticPositionEvent:
    node_id: str
    long_name: str | None
    short_name: str | None
    latitude: float
    longitude: float
    altitude: float | None
    timestamp: datetime
    channel: int
    source: str = "position_packet"
    cot_type: str | None = None
    speed: float | None = None
    course: float | None = None
    battery: float | None = None
    device_callsign: str | None = None
    team: str | None = None
    role: str | None = None
    node_role: str | None = None


@dataclass(frozen=True)
class MeshtasticChatEvent:
    node_id: str
    long_name: str | None
    short_name: str | None
    message_text: str
    timestamp: datetime
    channel: int


@dataclass(frozen=True)
class NodeBinding:
    node_id: str
    object_destination_hash: str
    display_name: str


@dataclass(frozen=True)
class BridgeStatusSnapshot:
    state: str
    meshtastic_connected: bool
    observed_nodes: int
    last_packet: datetime | None

    def to_dict(self) -> dict[str, object]:
        return {
            "state": self.state,
            "meshtastic_connected": self.meshtastic_connected,
            "observed_nodes": self.observed_nodes,
            "last_packet": to_rfc3339(self.last_packet),
        }
