from __future__ import annotations

import time
from typing import Any

from rch_mesh_bridge.config import MeshtasticConfig
from rch_mesh_bridge.meshtastic_adapter import MeshtasticAdapter


class FakeInterface:
    def __init__(self) -> None:
        self.closed = False
        self.nodesByNum: dict[int, dict[str, Any]] = {}

    def close(self) -> None:
        self.closed = True


class FakePub:
    def __init__(self) -> None:
        self._topics: dict[str, list[Any]] = {}

    def subscribe(self, callback, topic: str) -> None:
        self._topics.setdefault(topic, []).append(callback)

    def unsubscribe(self, callback, topic: str) -> None:
        callbacks = self._topics.get(topic, [])
        if callback in callbacks:
            callbacks.remove(callback)

    def sendMessage(self, topic: str, **kwargs: Any) -> None:
        for callback in list(self._topics.get(topic, [])):
            callback(**kwargs)


def _build_adapter(
    *,
    channel: int = 0,
    interface_factory=None,
    pubsub=None,
):
    position_events = []
    chat_events = []
    connection_events = []
    adapter = MeshtasticAdapter(
        MeshtasticConfig(host="127.0.0.1", port=4403, channel=channel, reconnect_max_seconds=4),
        on_position=position_events.append,
        on_chat=chat_events.append,
        on_connection_state=connection_events.append,
        interface_factory=interface_factory,
        pubsub=pubsub,
        sleep_fn=lambda _: None,
        jitter_fn=lambda _: 0.0,
    )
    return adapter, position_events, chat_events, connection_events


def test_position_packet_is_mapped() -> None:
    fake_interface = FakeInterface()
    fake_interface.nodesByNum[123] = {
        "user": {"longName": "LongName", "shortName": "SN", "id": "!abcd"}
    }
    adapter, position_events, _, _ = _build_adapter()
    adapter._interface = fake_interface

    adapter._on_position_packet(
        packet={
            "from": 123,
            "fromId": "!abcd",
            "channel": 0,
            "rxTime": 1700000000,
            "decoded": {"position": {"latitude": 12.34, "longitude": 56.78, "altitude": 90.0}},
        },
        interface=fake_interface,
    )

    assert len(position_events) == 1
    event = position_events[0]
    assert event.node_id == "!abcd"
    assert event.long_name == "LongName"
    assert event.latitude == 12.34
    assert event.longitude == 56.78
    assert event.altitude == 90.0


def test_channel_filter_rejects_non_matching_packets() -> None:
    fake_interface = FakeInterface()
    adapter, position_events, chat_events, _ = _build_adapter(channel=2)
    adapter._interface = fake_interface

    adapter._on_position_packet(
        packet={
            "from": 1,
            "channel": 0,
            "decoded": {"position": {"latitude": 1.0, "longitude": 2.0}},
        },
        interface=fake_interface,
    )
    adapter._on_text_packet(
        packet={
            "from": 1,
            "to": 0xFFFFFFFF,
            "channel": 0,
            "decoded": {"text": "hello"},
        },
        interface=fake_interface,
    )

    assert position_events == []
    assert chat_events == []


def test_direct_message_is_ignored() -> None:
    fake_interface = FakeInterface()
    adapter, _, chat_events, _ = _build_adapter(channel=0)
    adapter._interface = fake_interface

    adapter._on_text_packet(
        packet={
            "from": 12,
            "to": 34,
            "channel": 0,
            "decoded": {"text": "direct"},
        },
        interface=fake_interface,
    )

    assert chat_events == []


def test_disconnect_triggers_reconnect_backoff() -> None:
    fake_pub = FakePub()
    created_interfaces = []
    sleep_calls = []

    def factory(**kwargs):
        del kwargs
        iface = FakeInterface()
        created_interfaces.append(iface)
        return iface

    adapter, _, _, _ = _build_adapter(
        interface_factory=factory,
        pubsub=fake_pub,
    )
    adapter._sleep_fn = lambda seconds: sleep_calls.append(seconds)
    adapter.start()

    deadline = time.time() + 3.0
    while time.time() < deadline and not created_interfaces:
        time.sleep(0.02)

    assert created_interfaces, "Adapter did not create first interface"
    first = created_interfaces[0]
    fake_pub.sendMessage("meshtastic.connection.lost", interface=first)

    deadline = time.time() + 3.0
    while time.time() < deadline and len(created_interfaces) < 2:
        time.sleep(0.02)

    adapter.stop()

    assert len(created_interfaces) >= 2
    assert sleep_calls
    assert sleep_calls[0] >= 1.0
