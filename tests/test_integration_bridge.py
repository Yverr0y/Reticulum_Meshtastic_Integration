from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
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
from rch_mesh_bridge.models import MeshtasticPositionEvent
from rch_mesh_bridge.rch_client import RchClient, RchClientError
from rch_mesh_bridge.service import BridgeService


def _app_config(tmp_path: Path) -> AppConfig:
    return AppConfig(
        source_path=tmp_path / "config.ini",
        meshtastic=MeshtasticConfig(host="127.0.0.1", port=4403, channel=0, reconnect_max_seconds=2),
        rch=RchConfig(
            rest_url="http://rch.local",
            api_token="",
            identity="default",
            auth_mode="none",
            timeout_seconds=2.0,
        ),
        mapping=MappingConfig(
            chat_topic_path="meshtastic.channel.{channel}",
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
async def test_position_integration_create_then_reuse_marker(tmp_path: Path) -> None:
    requests = []
    markers = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8") or "{}") if request.content else {}
        requests.append((request.method, request.url.path, body))
        assert "Authorization" not in request.headers
        assert "X-API-Key" not in request.headers

        if request.method == "GET" and request.url.path == "/api/markers":
            return httpx.Response(200, json=markers)
        if request.method == "POST" and request.url.path == "/api/markers":
            marker = {
                "object_destination_hash": "obj-1",
                "name": body.get("name", "node"),
                "notes": body.get("notes", ""),
            }
            markers.append(marker)
            return httpx.Response(201, json={"object_destination_hash": "obj-1"})
        if request.method == "PATCH" and request.url.path == "/api/markers/obj-1/position":
            return httpx.Response(200, json={"status": "ok", "updated_at": "2026-01-01T00:00:00Z"})

        return httpx.Response(404, json={"error": "not found"})

    transport = httpx.MockTransport(handler)
    config = _app_config(tmp_path)
    async_client = httpx.AsyncClient(base_url=config.rch.rest_url, transport=transport)
    rch_client = RchClient(config.rch, client=async_client)
    core = BridgeCore(config, rch_client)

    event = MeshtasticPositionEvent(
        node_id="!node1",
        long_name="Node One",
        short_name="n1",
        latitude=1.1,
        longitude=2.2,
        altitude=None,
        timestamp=datetime.now(tz=timezone.utc),
        channel=0,
    )

    await core.handle_position_event(event)
    await core.handle_position_event(event)
    await async_client.aclose()

    assert len([req for req in requests if req[1] == "/api/markers" and req[0] == "POST"]) == 1
    assert len([req for req in requests if req[0] == "PATCH"]) == 2


@pytest.mark.asyncio
async def test_position_marker_create_retries_with_supported_symbol_on_422(
    tmp_path: Path,
) -> None:
    requests = []
    marker_create_calls = 0
    patch_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal marker_create_calls, patch_calls
        body = json.loads(request.content.decode("utf-8") or "{}") if request.content else {}
        requests.append((request.method, request.url.path, body))

        if request.method == "GET" and request.url.path == "/api/markers":
            return httpx.Response(200, json=[])
        if request.method == "GET" and request.url.path == "/api/markers/symbols":
            return httpx.Response(
                200,
                json=[
                    {"id": "map-marker", "set": "mdi"},
                    {"id": "flag", "set": "mdi"},
                ],
            )
        if request.method == "POST" and request.url.path == "/api/markers":
            marker_create_calls += 1
            if marker_create_calls == 1:
                return httpx.Response(
                    422,
                    json={
                        "detail": [
                            {
                                "loc": ["body", "marker_type"],
                                "msg": "Unsupported marker type",
                                "input": body.get("type"),
                            },
                            {
                                "loc": ["body", "symbol"],
                                "msg": "Unsupported marker symbol",
                                "input": body.get("symbol"),
                            },
                        ]
                    },
                )
            assert body["type"] == "map-marker"
            assert body["symbol"] == "map-marker"
            assert body["category"] == "mdi"
            return httpx.Response(201, json={"object_destination_hash": "obj-fallback"})
        if request.method == "PATCH" and request.url.path == "/api/markers/obj-fallback/position":
            patch_calls += 1
            return httpx.Response(200, json={"status": "ok", "updated_at": "2026-01-01T00:00:00Z"})

        return httpx.Response(404, json={"error": "not found"})

    transport = httpx.MockTransport(handler)
    config = _app_config(tmp_path)
    async_client = httpx.AsyncClient(base_url=config.rch.rest_url, transport=transport)
    rch_client = RchClient(config.rch, client=async_client)
    core = BridgeCore(config, rch_client)

    event = MeshtasticPositionEvent(
        node_id="!node1",
        long_name="Node One",
        short_name="n1",
        latitude=1.1,
        longitude=2.2,
        altitude=None,
        timestamp=datetime.now(tz=timezone.utc),
        channel=0,
    )

    await core.handle_position_event(event)
    await async_client.aclose()

    assert marker_create_calls == 2
    assert patch_calls == 1
    first_create = next(
        req for req in requests if req[0] == "POST" and req[1] == "/api/markers"
    )
    assert first_create[2]["type"] == "map-marker-account"
    assert first_create[2]["symbol"] == "map-marker-account"


@pytest.mark.asyncio
async def test_chat_integration_creates_topic_then_posts_message(tmp_path: Path) -> None:
    requests = []
    topics = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8") or "{}") if request.content else {}
        requests.append((request.method, request.url.path, body))

        if request.method == "GET" and request.url.path == "/Topic":
            return httpx.Response(200, json=topics)
        if request.method == "POST" and request.url.path == "/Topic":
            topic = {
                "TopicID": "topic-1",
                "TopicName": body.get("TopicName"),
                "TopicPath": body.get("TopicPath"),
            }
            topics.append(topic)
            return httpx.Response(200, json=topic)
        if request.method == "POST" and request.url.path == "/Message":
            return httpx.Response(200, json={"sent": True})

        return httpx.Response(404, json={"error": "not found"})

    transport = httpx.MockTransport(handler)
    config = _app_config(tmp_path)
    async_client = httpx.AsyncClient(base_url=config.rch.rest_url, transport=transport)
    rch_client = RchClient(config.rch, client=async_client)
    core = BridgeCore(config, rch_client)

    from rch_mesh_bridge.models import MeshtasticChatEvent

    event = MeshtasticChatEvent(
        node_id="!node1",
        long_name="Node One",
        short_name=None,
        message_text="hello world",
        timestamp=datetime.now(tz=timezone.utc),
        channel=0,
    )
    await core.handle_chat_event(event)
    await async_client.aclose()

    paths = [entry[1] for entry in requests]
    assert paths == ["/Topic", "/Topic", "/Message"]
    assert requests[-1][2]["Content"] == "Node One (!node1): hello world"


@pytest.mark.asyncio
async def test_rch_failure_is_single_attempt_without_retry(tmp_path: Path) -> None:
    patch_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal patch_calls
        if request.method == "POST" and request.url.path == "/api/markers":
            return httpx.Response(201, json={"object_destination_hash": "obj-1"})
        if request.method == "PATCH":
            patch_calls += 1
            return httpx.Response(500, json={"error": "boom"})
        return httpx.Response(200, json=[])

    transport = httpx.MockTransport(handler)
    config = _app_config(tmp_path)
    async_client = httpx.AsyncClient(base_url=config.rch.rest_url, transport=transport)
    rch_client = RchClient(config.rch, client=async_client)
    core = BridgeCore(config, rch_client)

    event = MeshtasticPositionEvent(
        node_id="!node1",
        long_name=None,
        short_name="n1",
        latitude=1.0,
        longitude=2.0,
        altitude=None,
        timestamp=datetime.now(tz=timezone.utc),
        channel=0,
    )
    with pytest.raises(RchClientError):
        await core.handle_position_event(event)

    await async_client.aclose()
    assert patch_calls == 1


class OneShotAdapter:
    def __init__(self, config, on_position, on_chat, on_connection_state):
        del config, on_chat
        self._on_position = on_position
        self._on_connection_state = on_connection_state

    def start(self) -> None:
        self._on_connection_state(True)
        self._on_position(
            MeshtasticPositionEvent(
                node_id="!node-service",
                long_name="Service Node",
                short_name="SN",
                latitude=5.0,
                longitude=6.0,
                altitude=None,
                timestamp=datetime.now(tz=timezone.utc),
                channel=0,
            )
        )

    def stop(self) -> None:
        self._on_connection_state(False)


@pytest.mark.asyncio
async def test_service_drops_failed_outbound_event(tmp_path: Path) -> None:
    patch_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal patch_calls
        if request.method == "GET" and request.url.path == "/api/markers":
            return httpx.Response(200, json=[])
        if request.method == "GET" and request.url.path == "/Topic":
            return httpx.Response(
                200,
                json=[
                    {
                        "TopicID": "topic-service",
                        "TopicPath": "meshtastic.channel.0",
                        "TopicName": "Meshtastic Channel 0",
                    }
                ],
            )
        if request.method == "POST" and request.url.path == "/api/markers":
            return httpx.Response(201, json={"object_destination_hash": "obj-service"})
        if request.method == "PATCH" and request.url.path == "/api/markers/obj-service/position":
            patch_calls += 1
            return httpx.Response(500, json={"error": "fail"})
        return httpx.Response(404, json={"error": "not found"})

    config = _app_config(tmp_path)
    async_client = httpx.AsyncClient(
        base_url=config.rch.rest_url,
        transport=httpx.MockTransport(handler),
    )

    def rch_factory(rch_cfg):
        return RchClient(rch_cfg, client=async_client)

    service = BridgeService(
        config,
        rch_client_factory=rch_factory,
        adapter_factory=OneShotAdapter,
    )
    stop_event = threading.Event()

    def stopper() -> None:
        time.sleep(0.6)
        stop_event.set()

    thread = threading.Thread(target=stopper, daemon=True)
    thread.start()
    await service.run(stop_event)
    await async_client.aclose()

    assert patch_calls == 1
