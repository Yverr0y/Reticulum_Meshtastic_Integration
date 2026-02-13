from __future__ import annotations

import json
from typing import Any
from urllib.parse import quote

import httpx

from .config import RchConfig


class RchClientError(RuntimeError):
    """Raised when an RCH request fails."""


class RchClient:
    def __init__(
        self,
        config: RchConfig,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._config = config
        self._client = client
        self._external_client = client is not None

        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=config.rest_url,
                timeout=config.timeout_seconds,
                headers=self._build_headers(config),
            )
        else:
            self._client.headers.update(self._build_headers(config))

    async def __aenter__(self) -> "RchClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def close(self) -> None:
        if self._client is not None and not self._external_client:
            await self._client.aclose()

    @staticmethod
    def _build_headers(config: RchConfig) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if config.auth_mode == "bearer":
            headers["Authorization"] = f"Bearer {config.api_token}"
        else:
            headers["X-API-Key"] = config.api_token
        return headers

    async def list_markers(self) -> list[dict[str, Any]]:
        data = await self._request_json("GET", "/api/markers", expected_status={200})
        if not isinstance(data, list):
            raise RchClientError("Unexpected /api/markers response (expected list)")
        return data

    async def create_marker(self, marker_payload: dict[str, Any]) -> dict[str, Any]:
        data = await self._request_json(
            "POST",
            "/api/markers",
            json_body=marker_payload,
            expected_status={200, 201},
        )
        if not isinstance(data, dict):
            raise RchClientError("Unexpected /api/markers create response")
        return data

    async def update_marker_position(
        self,
        object_destination_hash: str,
        latitude: float,
        longitude: float,
    ) -> dict[str, Any]:
        safe_hash = quote(object_destination_hash, safe="")
        data = await self._request_json(
            "PATCH",
            f"/api/markers/{safe_hash}/position",
            json_body={"lat": latitude, "lon": longitude},
            expected_status={200},
        )
        if not isinstance(data, dict):
            raise RchClientError("Unexpected marker position response")
        return data

    async def list_topics(self) -> list[dict[str, Any]]:
        data = await self._request_json("GET", "/Topic", expected_status={200})
        if not isinstance(data, list):
            raise RchClientError("Unexpected /Topic response (expected list)")
        return data

    async def create_topic(
        self,
        topic_name: str,
        topic_path: str,
        topic_description: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "TopicName": topic_name,
            "TopicPath": topic_path,
        }
        if topic_description:
            payload["TopicDescription"] = topic_description

        data = await self._request_json(
            "POST",
            "/Topic",
            json_body=payload,
            expected_status={200, 201},
        )
        if not isinstance(data, dict):
            raise RchClientError("Unexpected /Topic create response")
        return data

    async def send_message(self, content: str, topic_id: str) -> dict[str, Any]:
        payload = {"Content": content, "TopicID": topic_id}
        data = await self._request_json(
            "POST",
            "/Message",
            json_body=payload,
            expected_status={200},
        )
        if not isinstance(data, dict):
            raise RchClientError("Unexpected /Message response")
        return data

    async def _request_json(
        self,
        method: str,
        path: str,
        expected_status: set[int],
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        if self._client is None:
            raise RchClientError("HTTP client not initialized")

        try:
            response = await self._client.request(method, path, json=json_body)
        except httpx.TimeoutException as exc:
            raise RchClientError(f"Timeout calling {method} {path}") from exc
        except httpx.HTTPError as exc:
            raise RchClientError(f"Transport error calling {method} {path}: {exc!s}") from exc

        if response.status_code not in expected_status:
            detail = response.text.strip()
            raise RchClientError(
                f"RCH error {response.status_code} for {method} {path}: {detail}"
            )

        if not response.content:
            return {}

        try:
            return response.json()
        except json.JSONDecodeError as exc:
            raise RchClientError(f"Invalid JSON from {method} {path}") from exc

