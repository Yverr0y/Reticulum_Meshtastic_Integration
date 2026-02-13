from __future__ import annotations

import logging
import random
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable

from .config import MeshtasticConfig
from .models import MeshtasticChatEvent, MeshtasticPositionEvent


LOG = logging.getLogger(__name__)
BROADCAST_NUM = 0xFFFFFFFF


class MeshtasticAdapter:
    def __init__(
        self,
        config: MeshtasticConfig,
        on_position: Callable[[MeshtasticPositionEvent], None],
        on_chat: Callable[[MeshtasticChatEvent], None],
        on_connection_state: Callable[[bool], None],
        *,
        interface_factory: Callable[..., Any] | None = None,
        pubsub: Any | None = None,
        sleep_fn: Callable[[float], None] = time.sleep,
        jitter_fn: Callable[[float], float] | None = None,
    ) -> None:
        self._config = config
        self._on_position = on_position
        self._on_chat = on_chat
        self._on_connection_state = on_connection_state
        self._sleep_fn = sleep_fn
        self._jitter_fn = jitter_fn or (lambda base: random.uniform(0.0, base * 0.25))
        self._stop_event = threading.Event()
        self._connection_lost = threading.Event()
        self._thread: threading.Thread | None = None
        self._interface: Any | None = None
        self._connected = False

        if interface_factory is None:
            from meshtastic.tcp_interface import TCPInterface

            interface_factory = TCPInterface
        self._interface_factory = interface_factory

        if pubsub is None:
            from pubsub import pub

            pubsub = pub
        self._pub = pubsub
        self._subscribed = False

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._connection_lost.clear()
        self._register_subscriptions()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="meshtastic-adapter",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._connection_lost.set()
        self._close_interface()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        self._set_connected(False)
        self._unregister_subscriptions()

    def _register_subscriptions(self) -> None:
        if self._subscribed:
            return
        self._pub.subscribe(self._on_position_packet, "meshtastic.receive.position")
        self._pub.subscribe(self._on_text_packet, "meshtastic.receive.text")
        self._pub.subscribe(
            self._on_connection_established, "meshtastic.connection.established"
        )
        self._pub.subscribe(self._on_connection_lost, "meshtastic.connection.lost")
        self._subscribed = True

    def _unregister_subscriptions(self) -> None:
        if not self._subscribed:
            return
        for topic, handler in (
            ("meshtastic.receive.position", self._on_position_packet),
            ("meshtastic.receive.text", self._on_text_packet),
            ("meshtastic.connection.established", self._on_connection_established),
            ("meshtastic.connection.lost", self._on_connection_lost),
        ):
            try:
                self._pub.unsubscribe(handler, topic)
            except Exception:
                pass
        self._subscribed = False

    def _run_loop(self) -> None:
        backoff = 1.0
        while not self._stop_event.is_set():
            try:
                self._connection_lost.clear()
                self._interface = self._interface_factory(
                    hostname=self._config.host,
                    portNumber=self._config.port,
                )
                self._set_connected(True)
                backoff = 1.0

                while not self._stop_event.is_set():
                    if self._connection_lost.wait(timeout=0.2):
                        break

            except Exception:
                LOG.exception(
                    "Meshtastic connection failed (host=%s, port=%s)",
                    self._config.host,
                    self._config.port,
                )
            finally:
                self._set_connected(False)
                self._close_interface()

            if self._stop_event.is_set():
                break

            sleep_for = min(backoff, float(self._config.reconnect_max_seconds))
            sleep_for += max(0.0, self._jitter_fn(sleep_for))
            self._sleep_fn(sleep_for)
            backoff = min(
                backoff * 2,
                float(self._config.reconnect_max_seconds),
            )

    def _on_connection_established(self, interface: Any = None, **_: Any) -> None:
        if interface is self._interface:
            self._set_connected(True)

    def _on_connection_lost(self, interface: Any = None, **_: Any) -> None:
        if interface is self._interface:
            self._set_connected(False)
            self._connection_lost.set()

    def _on_position_packet(self, packet: dict[str, Any], interface: Any = None, **_: Any) -> None:
        if interface is not self._interface:
            return

        try:
            if self._packet_channel(packet) != self._config.channel:
                return

            decoded = packet.get("decoded", {})
            position = decoded.get("position", {})
            latitude = self._float_or_none(position.get("latitude"))
            longitude = self._float_or_none(position.get("longitude"))
            if latitude is None and "latitudeI" in position:
                latitude = self._float_or_none(position.get("latitudeI"), scale=1e-7)
            if longitude is None and "longitudeI" in position:
                longitude = self._float_or_none(position.get("longitudeI"), scale=1e-7)
            if latitude is None or longitude is None:
                return

            altitude = self._float_or_none(position.get("altitude"))
            timestamp = self._parse_timestamp(position.get("time"), packet.get("rxTime"))
            node_id, long_name, short_name = self._resolve_node_identity(packet, interface)

            event = MeshtasticPositionEvent(
                node_id=node_id,
                long_name=long_name,
                short_name=short_name,
                latitude=latitude,
                longitude=longitude,
                altitude=altitude,
                timestamp=timestamp,
                channel=self._config.channel,
            )
            self._on_position(event)
        except Exception:
            LOG.exception("Invalid Meshtastic position packet ignored")

    def _on_text_packet(self, packet: dict[str, Any], interface: Any = None, **_: Any) -> None:
        if interface is not self._interface:
            return

        try:
            if self._packet_channel(packet) != self._config.channel:
                return

            if not self._is_broadcast(packet):
                return

            message_text = self._decode_text(packet)
            if not message_text:
                return

            timestamp = self._parse_timestamp(None, packet.get("rxTime"))
            node_id, long_name, short_name = self._resolve_node_identity(packet, interface)
            event = MeshtasticChatEvent(
                node_id=node_id,
                long_name=long_name,
                short_name=short_name,
                message_text=message_text,
                timestamp=timestamp,
                channel=self._config.channel,
            )
            self._on_chat(event)
        except Exception:
            LOG.exception("Invalid Meshtastic chat packet ignored")

    def _resolve_node_identity(
        self,
        packet: dict[str, Any],
        interface: Any,
    ) -> tuple[str, str | None, str | None]:
        from_num = packet.get("from")
        node_id = str(packet.get("fromId") or "").strip()
        long_name = None
        short_name = None

        user: dict[str, Any] = {}
        nodes_by_num = getattr(interface, "nodesByNum", None)
        if isinstance(nodes_by_num, dict) and from_num in nodes_by_num:
            node_info = nodes_by_num.get(from_num)
            if isinstance(node_info, dict):
                maybe_user = node_info.get("user")
                if isinstance(maybe_user, dict):
                    user = maybe_user

        long_name = user.get("longName") or user.get("long_name")
        short_name = user.get("shortName") or user.get("short_name")

        if not node_id:
            user_id = user.get("id")
            if user_id:
                node_id = str(user_id)
            elif from_num is not None:
                node_id = str(from_num)
            else:
                node_id = "unknown"

        return node_id, long_name, short_name

    @staticmethod
    def _decode_text(packet: dict[str, Any]) -> str:
        decoded = packet.get("decoded", {})
        text = decoded.get("text")
        if isinstance(text, str):
            return text.strip()

        payload = decoded.get("payload")
        if isinstance(payload, bytes):
            return payload.decode("utf-8", errors="replace").strip()
        if isinstance(payload, str):
            return payload.strip()
        return ""

    @staticmethod
    def _parse_timestamp(primary: Any, fallback: Any) -> datetime:
        candidate = primary if primary is not None else fallback
        if candidate is not None:
            try:
                return datetime.fromtimestamp(float(candidate), tz=timezone.utc)
            except (TypeError, ValueError, OSError):
                pass
        return datetime.now(tz=timezone.utc)

    @staticmethod
    def _packet_channel(packet: dict[str, Any]) -> int:
        value = packet.get("channel", 0)
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _is_broadcast(packet: dict[str, Any]) -> bool:
        to_value = packet.get("to")
        if to_value is None:
            return True
        try:
            return int(to_value) == BROADCAST_NUM
        except (TypeError, ValueError):
            return False

    @staticmethod
    def _float_or_none(value: Any, *, scale: float = 1.0) -> float | None:
        if value is None:
            return None
        try:
            return float(value) * scale
        except (TypeError, ValueError):
            return None

    def _set_connected(self, value: bool) -> None:
        if self._connected == value:
            return
        self._connected = value
        try:
            self._on_connection_state(value)
        except Exception:
            LOG.exception("Connection callback failed")

    def _close_interface(self) -> None:
        interface = self._interface
        self._interface = None
        if interface is None:
            return
        try:
            interface.close()
        except Exception:
            pass

