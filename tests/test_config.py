from pathlib import Path

import pytest

from rch_mesh_bridge.config import BridgeConfigError, load_config


def _write_config(tmp_path: Path, body: str) -> Path:
    config_path = tmp_path / "config.ini"
    config_path.write_text(body, encoding="utf-8")
    return config_path


def test_load_config_applies_defaults(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        """
[meshtastic]
host = 127.0.0.1
""".strip(),
    )

    config = load_config(config_path)
    assert config.meshtastic.port == 4403
    assert config.meshtastic.channel == 0
    assert config.rch.rest_url == "http://localhost:8000"
    assert config.rch.auth_mode == "none"
    assert config.rch.api_token == ""
    assert config.rch.identity == "default"
    assert config.mapping.chat_topic_path == "meshtastic.channel.{channel}"
    assert config.runtime.pid_file.name == "bridge.pid"
    assert config.runtime.status_file.name == "status.json"


def test_missing_required_setting_raises(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        """
[meshtastic]
port = 4403

[rch]
rest_url = http://localhost:8080
api_token = abc
""".strip(),
    )

    with pytest.raises(BridgeConfigError):
        load_config(config_path)


def test_invalid_auth_mode_raises(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        """
[meshtastic]
host = 192.168.1.10

[rch]
rest_url = http://localhost:8080
auth_mode = invalid
""".strip(),
    )

    with pytest.raises(BridgeConfigError):
        load_config(config_path)


def test_invalid_topic_template_raises(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        """
[meshtastic]
host = 192.168.1.10

[rch]
rest_url = http://localhost:8080

[mapping]
topic_path_template = meshtastic.{unknown}
""".strip(),
    )

    with pytest.raises(BridgeConfigError):
        load_config(config_path)


def test_chat_topic_path_is_configurable(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        """
[meshtastic]
host = 192.168.1.10

[rch]
rest_url = http://localhost:8080

[mapping]
chat_topic_path = custom.topic.{channel}
""".strip(),
    )

    config = load_config(config_path)
    assert config.mapping.chat_topic_path == "custom.topic.{channel}"


def test_token_required_for_bearer_auth(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        """
[meshtastic]
host = 192.168.1.10

[rch]
rest_url = http://localhost:8080
auth_mode = bearer
""".strip(),
    )

    with pytest.raises(BridgeConfigError):
        load_config(config_path)
