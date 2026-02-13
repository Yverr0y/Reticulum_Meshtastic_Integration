from __future__ import annotations

import base64
import logging
import random
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable

from meshtastic.protobuf import atak_pb2, config_pb2, portnums_pb2

from .config import MeshtasticConfig
from .models import MeshtasticChatEvent, MeshtasticPositionEvent


LOG = logging.getLogger(__name__)
BROADCAST_NUM = 0xFFFFFFFF
REGULAR_COT_TYPE = "a-f-G-E-S"
ATAK_COT_TYPE = "a-f-G-U-C"
ATAK_PLUGIN_PORTNUM = int(portnums_pb2.PortNum.ATAK_PLUGIN)
ATAK_PLUGIN_PORTNAME = portnums_pb2.PortNum.Name(portnums_pb2.PortNum.ATAK_PLUGIN)


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
        self._connect_attempt = 0

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        LOG.info(
            "Starting Meshtastic adapter (host=%s port=%s channel=%s)",
            self._config.host,
            self._config.port,
            self._config.channel,
        )
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
        LOG.info("Stopping Meshtastic adapter")
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
        self._pub.subscribe(self._on_node_updated, "meshtastic.node.updated")
        self._pub.subscribe(
            self._on_atak_plugin_packet,
            f"meshtastic.receive.data.{ATAK_PLUGIN_PORTNAME}",
        )
        self._pub.subscribe(
            self._on_atak_plugin_packet,
            f"meshtastic.receive.data.{ATAK_PLUGIN_PORTNUM}",
        )
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
            ("meshtastic.node.updated", self._on_node_updated),
            (
                f"meshtastic.receive.data.{ATAK_PLUGIN_PORTNAME}",
                self._on_atak_plugin_packet,
            ),
            (
                f"meshtastic.receive.data.{ATAK_PLUGIN_PORTNUM}",
                self._on_atak_plugin_packet,
            ),
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
                self._connect_attempt += 1
                LOG.info(
                    "Connecting to Meshtastic (attempt=%s host=%s port=%s)",
                    self._connect_attempt,
                    self._config.host,
                    self._config.port,
                )
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
            LOG.info("Retrying Meshtastic connection in %.1f seconds", sleep_for)
            self._sleep_fn(sleep_for)
            backoff = min(
                backoff * 2,
                float(self._config.reconnect_max_seconds),
            )

    def _on_connection_established(self, interface: Any = None, **_: Any) -> None:
        if interface is self._interface:
            LOG.info("Meshtastic connection established")
            self._set_connected(True)

    def _on_connection_lost(self, interface: Any = None, **_: Any) -> None:
        if interface is self._interface:
            LOG.warning("Meshtastic connection lost")
            self._set_connected(False)
            self._connection_lost.set()

    def _on_position_packet(self, packet: dict[str, Any], interface: Any = None, **_: Any) -> None:
        if interface is not self._interface:
            return

        try:
            packet_channel = self._packet_channel(packet)
            if packet_channel != self._config.channel:
                LOG.debug(
                    "Ignoring position packet on channel=%s (expected=%s)",
                    packet_channel,
                    self._config.channel,
                )
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
                source="position_packet",
                cot_type=REGULAR_COT_TYPE,
            )
            LOG.info(
                "Meshtastic position rx source=position_packet node=%s name=%s lat=%.6f lon=%.6f alt=%s channel=%s",
                event.node_id,
                self._display_name(event.long_name, event.short_name, event.node_id),
                event.latitude,
                event.longitude,
                (
                    f"{event.altitude:.1f}m"
                    if event.altitude is not None
                    else "n/a"
                ),
                event.channel,
            )
            self._on_position(event)
        except Exception:
            LOG.exception("Invalid Meshtastic position packet ignored")

    def _on_node_updated(self, node: dict[str, Any], interface: Any = None, **_: Any) -> None:
        if interface is not self._interface:
            return

        try:
            if not isinstance(node, dict):
                return

            node_channel = self._node_channel(node)
            if node_channel != self._config.channel:
                LOG.debug(
                    "Ignoring node update on channel=%s (expected=%s)",
                    node_channel,
                    self._config.channel,
                )
                return

            position = node.get("position")
            if not isinstance(position, dict):
                return

            latitude = self._float_or_none(position.get("latitude"))
            longitude = self._float_or_none(position.get("longitude"))
            if latitude is None and "latitudeI" in position:
                latitude = self._float_or_none(position.get("latitudeI"), scale=1e-7)
            if longitude is None and "longitudeI" in position:
                longitude = self._float_or_none(position.get("longitudeI"), scale=1e-7)
            if latitude is None or longitude is None:
                return

            user = node.get("user") if isinstance(node.get("user"), dict) else {}
            node_id = str(user.get("id") or node.get("num") or "unknown")
            long_name = self._str_or_none(user.get("longName") or user.get("long_name"))
            short_name = self._str_or_none(user.get("shortName") or user.get("short_name"))
            node_role = self._extract_node_role(user)
            altitude = self._float_or_none(position.get("altitude"))
            battery = self._battery_from_node(node)
            timestamp = self._parse_timestamp(position.get("time"), node.get("lastHeard"))

            event = MeshtasticPositionEvent(
                node_id=node_id,
                long_name=long_name,
                short_name=short_name,
                latitude=latitude,
                longitude=longitude,
                altitude=altitude,
                timestamp=timestamp,
                channel=self._config.channel,
                source="node_update",
                cot_type=REGULAR_COT_TYPE,
                battery=battery,
                node_role=node_role,
            )
            LOG.info(
                "Meshtastic position rx source=node_update node=%s name=%s role=%s lat=%.6f lon=%.6f alt=%s battery=%s channel=%s",
                event.node_id,
                self._display_name(event.long_name, event.short_name, event.node_id),
                event.node_role or "n/a",
                event.latitude,
                event.longitude,
                (
                    f"{event.altitude:.1f}m"
                    if event.altitude is not None
                    else "n/a"
                ),
                (
                    f"{event.battery:.1f}%%"
                    if event.battery is not None
                    else "n/a"
                ),
                event.channel,
            )
            self._on_position(event)
        except Exception:
            LOG.exception("Invalid Meshtastic node update ignored")

    def _on_atak_plugin_packet(
        self,
        packet: dict[str, Any],
        interface: Any = None,
        **_: Any,
    ) -> None:
        if interface is not self._interface:
            return

        try:
            packet_channel = self._packet_channel(packet)
            if packet_channel != self._config.channel:
                LOG.debug(
                    "Ignoring ATAK packet on channel=%s (expected=%s)",
                    packet_channel,
                    self._config.channel,
                )
                return

            decoded_portnum = self._decoded_portnum(packet)
            if decoded_portnum is not None and decoded_portnum != ATAK_PLUGIN_PORTNUM:
                LOG.debug(
                    "Ignoring non-ATAK data packet on portnum=%s",
                    decoded_portnum,
                )
                return

            tak_packet = self._decode_tak_packet(packet)
            if tak_packet is None:
                return
            if not tak_packet.HasField("pli"):
                LOG.debug("Ignoring ATAK packet without PLI payload")
                return

            pli = tak_packet.pli
            latitude = self._float_or_none(pli.latitude_i, scale=1e-7)
            longitude = self._float_or_none(pli.longitude_i, scale=1e-7)
            if latitude is None or longitude is None:
                return

            altitude = self._float_or_none(pli.altitude)
            speed = self._float_or_none(pli.speed)
            course = self._float_or_none(pli.course)
            timestamp = self._parse_timestamp(None, packet.get("rxTime"))
            node_id, long_name, short_name = self._resolve_node_identity(packet, interface)

            contact_callsign = None
            device_callsign = None
            if tak_packet.HasField("contact"):
                contact_callsign = self._str_or_none(tak_packet.contact.callsign)
                device_callsign = self._str_or_none(tak_packet.contact.device_callsign)
            if contact_callsign:
                long_name = contact_callsign

            team = None
            role = None
            if tak_packet.HasField("group"):
                team = self._enum_name_or_none(atak_pb2.Team, tak_packet.group.team)
                role = self._enum_name_or_none(atak_pb2.MemberRole, tak_packet.group.role)

            battery = None
            if tak_packet.HasField("status"):
                battery = self._float_or_none(tak_packet.status.battery)

            event = MeshtasticPositionEvent(
                node_id=node_id,
                long_name=long_name,
                short_name=short_name,
                latitude=latitude,
                longitude=longitude,
                altitude=altitude,
                timestamp=timestamp,
                channel=self._config.channel,
                source="atak_pli",
                cot_type=ATAK_COT_TYPE,
                speed=speed,
                course=course,
                battery=battery,
                device_callsign=device_callsign,
                team=team,
                role=role,
            )
            LOG.info(
                "Meshtastic position rx source=atak_pli node=%s name=%s lat=%.6f lon=%.6f alt=%s speed=%s course=%s team=%s role=%s channel=%s",
                event.node_id,
                self._display_name(event.long_name, event.short_name, event.node_id),
                event.latitude,
                event.longitude,
                (
                    f"{event.altitude:.1f}m"
                    if event.altitude is not None
                    else "n/a"
                ),
                (
                    f"{event.speed:.1f}"
                    if event.speed is not None
                    else "n/a"
                ),
                (
                    f"{event.course:.1f}"
                    if event.course is not None
                    else "n/a"
                ),
                event.team or "n/a",
                event.role or "n/a",
                event.channel,
            )
            self._on_position(event)
        except Exception:
            LOG.exception("Invalid Meshtastic ATAK plugin packet ignored")

    def _on_text_packet(self, packet: dict[str, Any], interface: Any = None, **_: Any) -> None:
        if interface is not self._interface:
            return

        try:
            packet_channel = self._packet_channel(packet)
            if packet_channel != self._config.channel:
                LOG.debug(
                    "Ignoring text packet on channel=%s (expected=%s)",
                    packet_channel,
                    self._config.channel,
                )
                return

            if not self._is_broadcast(packet):
                LOG.debug("Ignoring direct text message packet")
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
            preview = event.message_text if len(event.message_text) <= 120 else f"{event.message_text[:117]}..."
            LOG.info(
                "Meshtastic chat rx node=%s name=%s channel=%s text=%s",
                event.node_id,
                self._display_name(event.long_name, event.short_name, event.node_id),
                event.channel,
                preview,
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
    def _str_or_none(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _enum_name_or_none(enum_type: Any, value: int) -> str | None:
        try:
            name = enum_type.Name(int(value))
        except Exception:
            return None
        if not name or name.lower().startswith("unspecif"):
            return None
        return str(name)

    @classmethod
    def _node_channel(cls, node: dict[str, Any]) -> int:
        return cls._int_or_default(node.get("channel"), 0)

    @classmethod
    def _battery_from_node(cls, node: dict[str, Any]) -> float | None:
        metrics = node.get("deviceMetrics")
        if not isinstance(metrics, dict):
            return None
        for key in ("batteryLevel", "battery_level", "battery"):
            if key in metrics:
                value = cls._float_or_none(metrics.get(key))
                if value is not None:
                    return value
        return None

    def _decode_tak_packet(self, packet: dict[str, Any]) -> atak_pb2.TAKPacket | None:
        decoded = packet.get("decoded")
        if not isinstance(decoded, dict):
            return None
        payload = decoded.get("payload")
        if payload is None:
            return None

        payload_bytes = self._payload_to_bytes(payload)
        if not payload_bytes:
            return None

        tak_packet = atak_pb2.TAKPacket()
        try:
            tak_packet.ParseFromString(payload_bytes)
        except Exception:
            LOG.debug("Failed to decode ATAK plugin payload", exc_info=True)
            return None
        return tak_packet

    @staticmethod
    def _decoded_portnum(packet: dict[str, Any]) -> int | None:
        decoded = packet.get("decoded")
        if not isinstance(decoded, dict):
            return None
        value = decoded.get("portnum")
        if value is None:
            return None
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            try:
                return int(text)
            except ValueError:
                try:
                    return int(portnums_pb2.PortNum.Value(text))
                except Exception:
                    return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _extract_node_role(user: dict[str, Any]) -> str | None:
        role = user.get("role")
        if role is None:
            return None
        if isinstance(role, str):
            text = role.strip()
            if not text:
                return None
            if text.isdigit():
                role = int(text)
            else:
                return text
        try:
            return config_pb2.Config.DeviceConfig.Role.Name(int(role))
        except Exception:
            return None

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
        return MeshtasticAdapter._int_or_default(value, 0)

    @staticmethod
    def _int_or_default(value: Any, default: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

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

    @staticmethod
    def _payload_to_bytes(payload: Any) -> bytes | None:
        if isinstance(payload, bytes):
            return payload
        if isinstance(payload, bytearray):
            return bytes(payload)
        if isinstance(payload, str):
            encoded = payload.strip()
            if not encoded:
                return None
            try:
                return base64.b64decode(encoded, validate=True)
            except Exception:
                return encoded.encode("utf-8", errors="replace")
        return None

    @staticmethod
    def _display_name(long_name: str | None, short_name: str | None, node_id: str) -> str:
        return long_name or short_name or node_id

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
