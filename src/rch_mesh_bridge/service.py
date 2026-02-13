from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
from typing import Any, Callable

from .bridge_core import BridgeCore
from .config import AppConfig
from .meshtastic_adapter import MeshtasticAdapter
from .models import MeshtasticChatEvent, MeshtasticPositionEvent
from .rch_client import RchClient, RchClientError
from .status import RuntimeStatusStore


LOG = logging.getLogger(__name__)


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


class BridgeService:
    def __init__(
        self,
        config: AppConfig,
        *,
        rch_client_factory: Callable[..., RchClient] = RchClient,
        adapter_factory: Callable[..., MeshtasticAdapter] = MeshtasticAdapter,
    ) -> None:
        self._config = config
        self._rch_client_factory = rch_client_factory
        self._adapter_factory = adapter_factory

    async def run(self, stop_event: threading.Event) -> None:
        LOG.info(
            "Bridge service starting (meshtastic=%s:%s channel=%s rch=%s auth_mode=%s)",
            self._config.meshtastic.host,
            self._config.meshtastic.port,
            self._config.meshtastic.channel,
            self._config.rch.rest_url,
            self._config.rch.auth_mode,
        )
        status_store = RuntimeStatusStore(self._config.runtime.status_file)
        event_queue: asyncio.Queue[tuple[str, Any]] = asyncio.Queue()
        loop = asyncio.get_running_loop()

        def on_position(event: MeshtasticPositionEvent) -> None:
            loop.call_soon_threadsafe(event_queue.put_nowait, ("position", event))

        def on_chat(event: MeshtasticChatEvent) -> None:
            loop.call_soon_threadsafe(event_queue.put_nowait, ("chat", event))

        def on_connection_state(connected: bool) -> None:
            status_store.set_meshtastic_connected(connected)
            LOG.info("Meshtastic connected=%s", connected)

        async with self._rch_client_factory(self._config.rch) as rch_client:
            core = BridgeCore(self._config, rch_client)
            try:
                await core.bootstrap_bindings()
                LOG.info("Bootstrapped %s existing Meshtastic node bindings", core.observed_nodes)
            except RchClientError as exc:
                LOG.warning("Bootstrap from RCH markers failed: %s", exc)

            status_store.set_observed_nodes(core.observed_nodes)
            status_store.write_snapshot(state=self._derive_state(status_store))

            processor_task = asyncio.create_task(
                self._event_processor(
                    event_queue=event_queue,
                    core=core,
                    status_store=status_store,
                )
            )
            status_task = asyncio.create_task(
                self._status_writer(stop_event=stop_event, status_store=status_store)
            )

            adapter = self._adapter_factory(
                self._config.meshtastic,
                on_position,
                on_chat,
                on_connection_state,
            )
            adapter.start()

            try:
                while not stop_event.is_set():
                    await asyncio.sleep(0.25)
            finally:
                LOG.info("Bridge service shutting down")
                adapter.stop()
                await event_queue.put(("shutdown", None))
                await processor_task
                status_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await status_task
                status_store.set_meshtastic_connected(False)
                status_store.write_snapshot(state="stopped")
                LOG.info("Bridge service stopped")

    async def _event_processor(
        self,
        *,
        event_queue: asyncio.Queue[tuple[str, Any]],
        core: BridgeCore,
        status_store: RuntimeStatusStore,
    ) -> None:
        while True:
            kind, payload = await event_queue.get()
            if kind == "shutdown":
                break

            try:
                if kind == "position" and isinstance(payload, MeshtasticPositionEvent):
                    await core.handle_position_event(payload)
                    status_store.mark_packet(payload.timestamp)
                    LOG.info(
                        "Forwarded position to RCH source=%s node=%s lat=%.6f lon=%.6f",
                        payload.source,
                        payload.node_id,
                        payload.latitude,
                        payload.longitude,
                    )
                elif kind == "chat" and isinstance(payload, MeshtasticChatEvent):
                    await core.handle_chat_event(payload)
                    status_store.mark_packet(payload.timestamp)
                    LOG.info(
                        "Forwarded chat to RCH node=%s text_len=%s",
                        payload.node_id,
                        len(payload.message_text),
                    )
            except RchClientError as exc:
                # V1 behavior is drop-on-failure. No persistence or replay.
                LOG.error("Dropped outbound payload due to RCH failure: %s", exc)
            except Exception:
                LOG.exception("Unexpected event processor error")
            finally:
                status_store.set_observed_nodes(core.observed_nodes)

    async def _status_writer(
        self,
        *,
        stop_event: threading.Event,
        status_store: RuntimeStatusStore,
    ) -> None:
        while not stop_event.is_set():
            status_store.write_snapshot(state=self._derive_state(status_store))
            await asyncio.sleep(2.0)

    @staticmethod
    def _derive_state(status_store: RuntimeStatusStore) -> str:
        return "running" if status_store.is_connected() else "degraded"
