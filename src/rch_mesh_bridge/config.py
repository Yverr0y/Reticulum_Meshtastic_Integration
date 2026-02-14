from __future__ import annotations

import configparser
from dataclasses import dataclass
from pathlib import Path


LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR"}
AUTH_MODES = {"none", "no_auth", "bearer", "x_api_key"}


class BridgeConfigError(ValueError):
    """Raised when config.ini values are missing or invalid."""


@dataclass(frozen=True)
class MeshtasticConfig:
    host: str
    port: int
    channel: int
    reconnect_max_seconds: int


@dataclass(frozen=True)
class RchConfig:
    rest_url: str
    api_token: str
    identity: str
    auth_mode: str
    timeout_seconds: float


@dataclass(frozen=True)
class MappingConfig:
    chat_topic_path: str
    topic_path_template: str
    topic_name_template: str
    marker_type: str
    marker_symbol: str
    marker_category: str
    marker_name_template: str
    node_tag_prefix: str


@dataclass(frozen=True)
class RuntimeConfig:
    pid_file: Path
    status_file: Path


@dataclass(frozen=True)
class GeneralConfig:
    log_level: str


@dataclass(frozen=True)
class AppConfig:
    source_path: Path
    meshtastic: MeshtasticConfig
    rch: RchConfig
    mapping: MappingConfig
    runtime: RuntimeConfig
    general: GeneralConfig


def _resolve_path(base_dir: Path, value: str) -> Path:
    candidate = Path(value).expanduser()
    if candidate.is_absolute():
        return candidate
    return (base_dir / candidate).resolve()


def _require(parser: configparser.ConfigParser, section: str, key: str) -> str:
    value = parser.get(section, key, fallback="").strip()
    if not value:
        raise BridgeConfigError(f"Missing required setting [{section}] {key}")
    return value


def _int_setting(
    parser: configparser.ConfigParser,
    section: str,
    key: str,
    default: int,
) -> int:
    raw = parser.get(section, key, fallback=str(default)).strip()
    try:
        return int(raw)
    except ValueError as exc:
        raise BridgeConfigError(f"Invalid integer for [{section}] {key}: {raw}") from exc


def _float_setting(
    parser: configparser.ConfigParser,
    section: str,
    key: str,
    default: float,
) -> float:
    raw = parser.get(section, key, fallback=str(default)).strip()
    try:
        return float(raw)
    except ValueError as exc:
        raise BridgeConfigError(f"Invalid float for [{section}] {key}: {raw}") from exc


def infer_runtime_paths(config_path: Path) -> RuntimeConfig:
    base_dir = config_path.expanduser().resolve().parent
    return RuntimeConfig(
        pid_file=(base_dir / ".runtime" / "bridge.pid").resolve(),
        status_file=(base_dir / ".runtime" / "status.json").resolve(),
    )


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path).expanduser().resolve()
    if not config_path.exists():
        raise BridgeConfigError(f"Config file not found: {config_path}")

    parser = configparser.ConfigParser(interpolation=None)
    parser.read(config_path, encoding="utf-8")

    host = _require(parser, "meshtastic", "host")
    port = _int_setting(parser, "meshtastic", "port", 4403)
    channel = _int_setting(parser, "meshtastic", "channel", 0)
    reconnect_max_seconds = _int_setting(
        parser, "runtime", "reconnect_max_seconds", 60
    )

    rest_url = parser.get("rch", "rest_url", fallback="http://localhost:8000").strip()
    if not rest_url:
        rest_url = "http://localhost:8000"
    identity = parser.get("rch", "identity", fallback="default").strip() or "default"
    auth_mode = parser.get("rch", "auth_mode", fallback="none").strip().lower()
    if auth_mode == "no_auth":
        auth_mode = "none"
    api_token = parser.get("rch", "api_token", fallback="").strip()
    timeout_seconds = _float_setting(parser, "rch", "timeout_seconds", 5.0)

    topic_path_template = parser.get(
        "mapping",
        "topic_path_template",
        fallback="meshtastic.channel.{channel}",
    ).strip()
    chat_topic_path = parser.get("mapping", "chat_topic_path", fallback="").strip()
    if not chat_topic_path:
        chat_topic_path = topic_path_template
    topic_name_template = parser.get(
        "mapping",
        "topic_name_template",
        fallback="Meshtastic Channel {channel}",
    ).strip()
    marker_type = parser.get(
        "mapping",
        "marker_type",
        fallback="auto",
    ).strip()
    marker_symbol = parser.get("mapping", "marker_symbol", fallback="auto").strip()
    marker_category = parser.get("mapping", "marker_category", fallback="mdi").strip()
    marker_name_template = parser.get(
        "mapping",
        "marker_name_template",
        fallback="{long_name|short_name|node_id}",
    ).strip()
    node_tag_prefix = parser.get(
        "mapping",
        "node_tag_prefix",
        fallback="meshtastic_node_id=",
    ).strip()

    pid_file_raw = parser.get("runtime", "pid_file", fallback=".runtime/bridge.pid").strip()
    status_file_raw = parser.get(
        "runtime", "status_file", fallback=".runtime/status.json"
    ).strip()

    log_level = parser.get("general", "log_level", fallback="INFO").strip().upper()

    if not 1 <= port <= 65535:
        raise BridgeConfigError(f"Invalid [meshtastic] port: {port}")
    if channel < 0:
        raise BridgeConfigError(f"Invalid [meshtastic] channel: {channel}")
    if reconnect_max_seconds < 1:
        raise BridgeConfigError(
            f"Invalid [runtime] reconnect_max_seconds: {reconnect_max_seconds}"
        )

    if auth_mode not in AUTH_MODES:
        raise BridgeConfigError(
            f"Invalid [rch] auth_mode: {auth_mode}. Expected one of {sorted(AUTH_MODES)}"
        )
    if auth_mode in {"bearer", "x_api_key"} and not api_token:
        raise BridgeConfigError(
            f"Missing required setting [rch] api_token for auth_mode '{auth_mode}'"
        )
    if timeout_seconds <= 0:
        raise BridgeConfigError(
            f"Invalid [rch] timeout_seconds: {timeout_seconds}. Must be > 0."
        )

    if log_level not in LOG_LEVELS:
        raise BridgeConfigError(
            f"Invalid [general] log_level: {log_level}. Expected one of {sorted(LOG_LEVELS)}"
        )

    if not topic_path_template:
        raise BridgeConfigError("Setting [mapping] topic_path_template cannot be empty")
    if not chat_topic_path:
        raise BridgeConfigError("Setting [mapping] chat_topic_path cannot be empty")
    if not topic_name_template:
        raise BridgeConfigError("Setting [mapping] topic_name_template cannot be empty")
    if not marker_type:
        raise BridgeConfigError("Setting [mapping] marker_type cannot be empty")
    if not marker_symbol:
        raise BridgeConfigError("Setting [mapping] marker_symbol cannot be empty")
    if not marker_category:
        raise BridgeConfigError("Setting [mapping] marker_category cannot be empty")
    if not marker_name_template:
        raise BridgeConfigError("Setting [mapping] marker_name_template cannot be empty")
    if not node_tag_prefix:
        raise BridgeConfigError("Setting [mapping] node_tag_prefix cannot be empty")

    for label, template in (
        ("chat_topic_path", chat_topic_path),
        ("topic_path_template", topic_path_template),
        ("topic_name_template", topic_name_template),
    ):
        try:
            template.format(channel=0)
        except KeyError as exc:
            raise BridgeConfigError(
                f"Invalid [mapping] {label}: unknown format key {exc!s}"
            ) from exc
        except ValueError as exc:
            raise BridgeConfigError(f"Invalid [mapping] {label}: {exc!s}") from exc

    base_dir = config_path.parent
    runtime = RuntimeConfig(
        pid_file=_resolve_path(base_dir, pid_file_raw),
        status_file=_resolve_path(base_dir, status_file_raw),
    )

    return AppConfig(
        source_path=config_path,
        meshtastic=MeshtasticConfig(
            host=host,
            port=port,
            channel=channel,
            reconnect_max_seconds=reconnect_max_seconds,
        ),
        rch=RchConfig(
            rest_url=rest_url.rstrip("/"),
            api_token=api_token,
            identity=identity,
            auth_mode=auth_mode,
            timeout_seconds=timeout_seconds,
        ),
        mapping=MappingConfig(
            chat_topic_path=chat_topic_path,
            topic_path_template=topic_path_template,
            topic_name_template=topic_name_template,
            marker_type=marker_type,
            marker_symbol=marker_symbol,
            marker_category=marker_category,
            marker_name_template=marker_name_template,
            node_tag_prefix=node_tag_prefix,
        ),
        runtime=runtime,
        general=GeneralConfig(log_level=log_level),
    )
