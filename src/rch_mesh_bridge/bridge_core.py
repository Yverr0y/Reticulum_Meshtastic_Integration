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
    _FALLBACK_SYMBOL_PRIORITY = (
        "map-marker-account",
        "map-marker",
        "marker",
        "information",
        "circle",
        "star",
        "pin",
    )

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

            binding_key = self._extract_node_id(notes)
            if not binding_key:
                continue

            self._node_bindings[binding_key] = NodeBinding(
                node_id=self._node_id_from_binding_key(binding_key),
                object_destination_hash=str(object_hash),
                display_name=display_name or self._node_id_from_binding_key(binding_key),
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
        binding_key = self._binding_key_for_event(event)
        existing = self._node_bindings.get(binding_key)
        if existing:
            return existing

        display_name = self._format_display_name(
            long_name=event.long_name,
            short_name=event.short_name,
            node_id=event.node_id,
        )
        notes = self._build_marker_notes(event, binding_key)
        resolved_symbol = self._normalize_marker_symbol(self._config.mapping.marker_symbol)
        resolved_type = self._resolve_marker_type(
            configured_type=self._config.mapping.marker_type,
            resolved_symbol=resolved_symbol,
            event_cot_type=event.cot_type,
        )
        marker_payload = {
            "type": resolved_type,
            "symbol": resolved_symbol,
            "name": display_name,
            "category": self._config.mapping.marker_category,
            "lat": event.latitude,
            "lon": event.longitude,
            "notes": notes,
        }

        response = await self._create_marker_with_fallback(
            marker_payload=marker_payload,
            node_id=event.node_id,
        )
        object_hash = _pick(response, "object_destination_hash", "objectDestinationHash")
        if not object_hash:
            markers = await self._rch.list_markers()
            object_hash = self._find_marker_hash(markers, binding_key)

        if not object_hash:
            raise RchClientError(
                f"Unable to resolve marker object hash for node {event.node_id}"
            )

        binding = NodeBinding(
            node_id=event.node_id,
            object_destination_hash=str(object_hash),
            display_name=display_name,
        )
        self._node_bindings[binding_key] = binding
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
        self, markers: list[dict[str, Any]], binding_key: str
    ) -> str | None:
        for marker in markers:
            if not isinstance(marker, dict):
                continue
            notes = _pick(marker, "notes", "Notes")
            if not isinstance(notes, str):
                continue
            extracted = self._extract_node_id(notes)
            if extracted == binding_key:
                object_hash = _pick(
                    marker, "object_destination_hash", "objectDestinationHash"
                )
                if object_hash:
                    return str(object_hash)
        return None

    def _build_marker_notes(self, event: MeshtasticPositionEvent, binding_key: str) -> str:
        lines = [f"{self._config.mapping.node_tag_prefix}{binding_key}"]
        if event.source:
            lines.append(f"source={event.source}")
        if event.cot_type:
            lines.append(f"cot_type={event.cot_type}")
        if event.altitude is not None:
            lines.append(f"altitude={event.altitude}")
        if event.speed is not None:
            lines.append(f"speed={event.speed}")
        if event.course is not None:
            lines.append(f"course={event.course}")
        if event.battery is not None:
            lines.append(f"battery={event.battery}")
        if event.device_callsign:
            lines.append(f"device_callsign={event.device_callsign}")
        if event.team:
            lines.append(f"team={event.team}")
        if event.role:
            lines.append(f"role={event.role}")
        if event.node_role:
            lines.append(f"node_role={event.node_role}")
        return "\n".join(lines)

    def _format_display_name(
        self, long_name: str | None, short_name: str | None, node_id: str
    ) -> str:
        values = {"long_name": long_name, "short_name": short_name, "node_id": node_id}
        rendered = _render_template(self._config.mapping.marker_name_template, values)
        return rendered or node_id

    @staticmethod
    def _normalize_marker_symbol(value: str) -> str:
        if value.lower() == "auto":
            return "map-marker-account"
        return value

    @staticmethod
    def _resolve_marker_type(
        *,
        configured_type: str,
        resolved_symbol: str,
        event_cot_type: str | None,
    ) -> str:
        normalized = configured_type.strip().lower()
        if normalized in {"auto", "default", "meshtastic_node"}:
            if event_cot_type:
                return event_cot_type
            return resolved_symbol
        return configured_type

    @staticmethod
    def _binding_key_for_event(event: MeshtasticPositionEvent) -> str:
        if event.source == "atak_pli":
            return f"tak:{event.node_id}"
        return event.node_id

    @staticmethod
    def _node_id_from_binding_key(binding_key: str) -> str:
        if binding_key.startswith("tak:"):
            return binding_key.split(":", 1)[1] or binding_key
        return binding_key

    async def _create_marker_with_fallback(
        self,
        *,
        marker_payload: dict[str, Any],
        node_id: str,
    ) -> dict[str, Any]:
        try:
            return await self._rch.create_marker(marker_payload)
        except RchClientError as exc:
            if not self._is_marker_validation_error(exc):
                raise

            fallback_payload = await self._build_fallback_marker_payload(marker_payload)
            if fallback_payload == marker_payload:
                raise

            LOG.warning(
                "Marker payload rejected for node=%s (type=%s symbol=%s). "
                "Retrying with fallback type=%s symbol=%s category=%s",
                node_id,
                marker_payload.get("type"),
                marker_payload.get("symbol"),
                fallback_payload.get("type"),
                fallback_payload.get("symbol"),
                fallback_payload.get("category"),
            )
            return await self._rch.create_marker(fallback_payload)

    @staticmethod
    def _is_marker_validation_error(exc: RchClientError) -> bool:
        message = str(exc).lower()
        return (
            "rch error 422" in message
            and (
                "marker_type" in message
                or "symbol" in message
                or "unsupported marker" in message
            )
        )

    async def _build_fallback_marker_payload(
        self, payload: dict[str, Any]
    ) -> dict[str, Any]:
        symbols = await self._safe_list_marker_symbols()
        symbol_id, symbol_set = self._pick_fallback_symbol(symbols)

        fallback = dict(payload)
        if symbol_id:
            fallback["symbol"] = symbol_id
            fallback["type"] = symbol_id
        if symbol_set:
            fallback["category"] = symbol_set

        return fallback

    async def _safe_list_marker_symbols(self) -> list[dict[str, Any]]:
        try:
            symbols = await self._rch.list_marker_symbols()
            if symbols:
                LOG.info("Loaded %s marker symbols from RCH", len(symbols))
            return symbols
        except RchClientError as exc:
            LOG.warning("Could not load marker symbols from RCH: %s", exc)
            return []

    def _pick_fallback_symbol(
        self, symbols: list[dict[str, Any]]
    ) -> tuple[str | None, str | None]:
        if not symbols:
            return ("map-marker-account", self._config.mapping.marker_category)

        indexed: dict[str, str | None] = {}
        for symbol in symbols:
            symbol_id = _pick(symbol, "id", "symbol", "name")
            symbol_set = _pick(symbol, "set", "category")
            if symbol_id:
                indexed[str(symbol_id)] = str(symbol_set) if symbol_set else None

        for preferred in self._FALLBACK_SYMBOL_PRIORITY:
            if preferred in indexed:
                return preferred, indexed[preferred]

        first_id = next(iter(indexed), None)
        if first_id is None:
            return ("map-marker-account", self._config.mapping.marker_category)
        return first_id, indexed[first_id]
