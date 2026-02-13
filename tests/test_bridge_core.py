from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from rch_mesh_bridge.bridge_core import BridgeCore
from rch_mesh_bridge.config import (
    AppConfig,
    GeneralConfig,
    MappingConfig,
    MeshtasticConfig,
    RchConfig,
    RuntimeConfig,
)
from rch_mesh_bridge.models import MeshtasticChatEvent, MeshtasticPositionEvent


class FakeRchClient:
    def __init__(self) -> None:
        self.markers = []
        self.topics = []
        self.sent_messages = []
        self.position_updates = []

    async def list_markers(self):
        return list(self.markers)

    async def create_marker(self, marker_payload):
        marker_hash = "obj-created"
        record = {
            "object_destination_hash": marker_hash,
            "name": marker_payload["name"],
            "notes": marker_payload["notes"],
        }
        self.markers.append(record)
        return {"object_destination_hash": marker_hash, "created_at": "2026-02-13T00:00:00Z"}

    async def update_marker_position(self, object_destination_hash, latitude, longitude):
        self.position_updates.append((object_destination_hash, latitude, longitude))
        return {"status": "ok", "updated_at": "2026-02-13T00:00:00Z"}

    async def list_topics(self):
        return list(self.topics)

    async def create_topic(self, topic_name, topic_path, topic_description=None):
        del topic_description
        record = {"TopicID": "topic-created", "TopicName": topic_name, "TopicPath": topic_path}
        self.topics.append(record)
        return dict(record)

    async def send_message(self, content, topic_id):
        self.sent_messages.append((content, topic_id))
        return {"sent": True}


def _config(tmp_path: Path) -> AppConfig:
    return AppConfig(
        source_path=tmp_path / "config.ini",
        meshtastic=MeshtasticConfig(host="127.0.0.1", port=4403, channel=0, reconnect_max_seconds=60),
        rch=RchConfig(
            rest_url="http://localhost:8080",
            api_token="token",
            identity="default",
            auth_mode="bearer",
            timeout_seconds=5.0,
        ),
        mapping=MappingConfig(
            topic_path_template="meshtastic.channel.{channel}",
            topic_name_template="Meshtastic Channel {channel}",
            marker_type="meshtastic_node",
            marker_symbol="auto",
            marker_category="mdi",
            marker_name_template="{long_name|short_name|node_id}",
            node_tag_prefix="meshtastic_node_id=",
        ),
        runtime=RuntimeConfig(
            pid_file=tmp_path / ".runtime" / "bridge.pid",
            status_file=tmp_path / ".runtime" / "status.json",
        ),
        general=GeneralConfig(log_level="INFO"),
    )


@pytest.mark.asyncio
async def test_bootstrap_existing_marker_binding(tmp_path: Path) -> None:
    fake_client = FakeRchClient()
    fake_client.markers.append(
        {
            "object_destination_hash": "obj-1",
            "name": "Node 1",
            "notes": "foo meshtastic_node_id=!node1",
        }
    )
    core = BridgeCore(_config(tmp_path), fake_client)
    await core.bootstrap_bindings()

    assert core.observed_nodes == 1


@pytest.mark.asyncio
async def test_chat_creates_topic_and_sends_message(tmp_path: Path) -> None:
    fake_client = FakeRchClient()
    core = BridgeCore(_config(tmp_path), fake_client)

    event = MeshtasticChatEvent(
        node_id="!abcd",
        long_name="Tracker 1",
        short_name="T1",
        message_text="hello mesh",
        timestamp=datetime.now(tz=timezone.utc),
        channel=0,
    )
    await core.handle_chat_event(event)

    assert fake_client.topics
    assert fake_client.sent_messages
    message, topic_id = fake_client.sent_messages[0]
    assert message == "Tracker 1 (!abcd): hello mesh"
    assert topic_id == "topic-created"


@pytest.mark.asyncio
async def test_position_creates_marker_then_updates_position(tmp_path: Path) -> None:
    fake_client = FakeRchClient()
    core = BridgeCore(_config(tmp_path), fake_client)

    event = MeshtasticPositionEvent(
        node_id="!node-123",
        long_name=None,
        short_name="node123",
        latitude=10.0,
        longitude=20.0,
        altitude=30.0,
        timestamp=datetime.now(tz=timezone.utc),
        channel=0,
    )

    await core.handle_position_event(event)
    await core.handle_position_event(event)

    assert len(fake_client.markers) == 1
    assert len(fake_client.position_updates) == 2
    assert fake_client.position_updates[0][0] == "obj-created"

