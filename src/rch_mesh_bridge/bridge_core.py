from __future__ import annotations

import logging
import re
from typing import Any

from .config import AppConfig
from .models import MeshtasticChatEvent, MeshtasticPositionEvent, NodeBinding
from .rch_client import RchClient, RchClientError


LOG = logging.getLogger(__name__)

_PLACEHOLDER_PATTERN = re.compile(r"\{([^{}]+)\}")


def _pick(data: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in data and data[key] is not None:
            return data[key]
    return None


def _render_template(template: str, values: dict[str, str | None]) -> str:
    def repl(match: re.Match[str]) -> str:
        token = match.group(1)
        candidates = [entry.strip() for entry in token.split("|") if entry.strip()]
        for candidate in candidates:
            value = values.get(candidate)
            if value:
                return str(value)
        return ""

    rendered = _PLACEHOLDER_PATTERN.sub(repl, template)
    return rendered.strip()


class BridgeCore:
    def __init__(self, config: AppConfig, rch_client: RchClient) -> None:
        self._config = config
        self._rch = rch_client
        self._node_bindings: dict[str, NodeBinding] = {}
        self._topic_ids: dict[int, str] = {}

    @property
    def observed_nodes(self) -> int:
        return len(self._node_bindings)

    async def bootstrap_bindings(self) -> None:
        markers = await self._rch.list_markers()
        for marker in markers:
            if not isinstance(marker, dict):
                continue
            object_hash = _pick(marker, "object_destination_hash", "objectDestinationHash")
            notes = _pick(marker, "notes", "Notes")
            display_name = str(_pick(marker, "name", "Name") or "")
            if not object_hash or not notes:
                continue

            node_id = self._extract_node_id(notes)
            if not node_id:
                continue

            self._node_bindings[node_id] = NodeBinding(
                node_id=node_id,
                object_destination_hash=str(object_hash),
                display_name=display_name or node_id,
            )

    async def handle_position_event(self, event: MeshtasticPositionEvent) -> None:
        binding = await self._ensure_node_binding(event)
        await self._rch.update_marker_position(
            object_destination_hash=binding.object_destination_hash,
            latitude=event.latitude,
            longitude=event.longitude,
        )

    async def handle_chat_event(self, event: MeshtasticChatEvent) -> None:
        message = event.message_text.strip()
        if not message:
            return

        topic_id = await self._ensure_topic_id(event.channel)
        display_name = self._format_display_name(
            long_name=event.long_name,
            short_name=event.short_name,
            node_id=event.node_id,
        )
        normalized = f"{display_name} ({event.node_id}): {message}"
        await self._rch.send_message(content=normalized, topic_id=topic_id)

    async def _ensure_node_binding(
        self, event: MeshtasticPositionEvent
    ) -> NodeBinding:
        existing = self._node_bindings.get(event.node_id)
        if existing:
            return existing

        display_name = self._format_display_name(
            long_name=event.long_name,
            short_name=event.short_name,
            node_id=event.node_id,
        )
        notes = f"{self._config.mapping.node_tag_prefix}{event.node_id}"
        marker_payload = {
            "type": self._config.mapping.marker_type,
            "symbol": self._normalize_marker_symbol(self._config.mapping.marker_symbol),
            "name": display_name,
            "category": self._config.mapping.marker_category,
            "lat": event.latitude,
            "lon": event.longitude,
            "notes": notes,
        }

        response = await self._rch.create_marker(marker_payload)
        object_hash = _pick(response, "object_destination_hash", "objectDestinationHash")
        if not object_hash:
            markers = await self._rch.list_markers()
            object_hash = self._find_marker_hash(markers, event.node_id)

        if not object_hash:
            raise RchClientError(
                f"Unable to resolve marker object hash for node {event.node_id}"
            )

        binding = NodeBinding(
            node_id=event.node_id,
            object_destination_hash=str(object_hash),
            display_name=display_name,
        )
        self._node_bindings[event.node_id] = binding
        return binding

    async def _ensure_topic_id(self, channel: int) -> str:
        cached = self._topic_ids.get(channel)
        if cached:
            return cached

        topic_path = self._config.mapping.topic_path_template.format(channel=channel)
        topic_name = self._config.mapping.topic_name_template.format(channel=channel)

        topics = await self._rch.list_topics()
        for topic in topics:
            if not isinstance(topic, dict):
                continue
            path = _pick(topic, "TopicPath", "topic_path", "path")
            if path == topic_path:
                topic_id = _pick(topic, "TopicID", "topic_id", "id")
                if topic_id:
                    self._topic_ids[channel] = str(topic_id)
                    return str(topic_id)

        created = await self._rch.create_topic(
            topic_name=topic_name,
            topic_path=topic_path,
            topic_description=f"Auto-created Meshtastic bridge topic for channel {channel}",
        )
        topic_id = _pick(created, "TopicID", "topic_id", "id")
        if not topic_id:
            topics = await self._rch.list_topics()
            for topic in topics:
                if not isinstance(topic, dict):
                    continue
                if _pick(topic, "TopicPath", "topic_path", "path") == topic_path:
                    topic_id = _pick(topic, "TopicID", "topic_id", "id")
                    if topic_id:
                        break

        if not topic_id:
            raise RchClientError(f"Unable to resolve TopicID for topic path {topic_path}")

        self._topic_ids[channel] = str(topic_id)
        return str(topic_id)

    def _extract_node_id(self, notes: str) -> str | None:
        prefix = self._config.mapping.node_tag_prefix
        idx = notes.find(prefix)
        if idx == -1:
            return None
        tail = notes[idx + len(prefix) :]
        match = re.match(r"([^\s,;]+)", tail)
        if not match:
            return None
        return match.group(1).strip()

    def _find_marker_hash(
        self, markers: list[dict[str, Any]], node_id: str
    ) -> str | None:
        for marker in markers:
            if not isinstance(marker, dict):
                continue
            notes = _pick(marker, "notes", "Notes")
            if not isinstance(notes, str):
                continue
            extracted = self._extract_node_id(notes)
            if extracted == node_id:
                object_hash = _pick(
                    marker, "object_destination_hash", "objectDestinationHash"
                )
                if object_hash:
                    return str(object_hash)
        return None

    def _format_display_name(
        self, long_name: str | None, short_name: str | None, node_id: str
    ) -> str:
        values = {"long_name": long_name, "short_name": short_name, "node_id": node_id}
        rendered = _render_template(self._config.mapping.marker_name_template, values)
        return rendered or node_id

    @staticmethod
    def _normalize_marker_symbol(value: str) -> str:
        if value.lower() == "auto":
            return "information"
        return value
