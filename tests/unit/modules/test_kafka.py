from unittest.mock import Mock

import pytest

import saltext.kafka.modules.kafka_mod as kafka_module


def test_request_closes_client():
    admin = Mock()
    admin.describe_cluster.return_value = {"cluster_id": "cluster-a"}
    client = Mock(return_value=admin)
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(kafka_module, "_client", client)
    try:
        result = kafka_module.cluster_info("broker:9092", {"security_protocol": "SSL"})
    finally:
        monkeypatch.undo()
    assert result == {"cluster_id": "cluster-a"}
    client.assert_called_once_with("broker:9092", {"security_protocol": "SSL"})
    admin.close.assert_called_once_with()


def test_request_closes_client_on_error(monkeypatch):
    admin = Mock()
    admin.describe_cluster.side_effect = RuntimeError("broker unavailable")
    monkeypatch.setattr(kafka_module, "_client", Mock(return_value=admin))
    with pytest.raises(RuntimeError, match="broker unavailable"):
        kafka_module.cluster_info()
    admin.close.assert_called_once_with()


def test_topic_info_finds_requested_topic(monkeypatch):
    monkeypatch.setattr(
        kafka_module,
        "_request",
        Mock(return_value=[{"topic": "events"}, {"topic": "metrics"}]),
    )
    assert kafka_module.topic_info("metrics") == {"topic": "metrics"}


def test_topic_info_returns_none_for_absent_topic(monkeypatch):
    monkeypatch.setattr(kafka_module, "_request", Mock(return_value=[]))
    assert kafka_module.topic_info("missing") is None


def test_topic_create_rejects_non_positive_counts():
    with pytest.raises(ValueError, match="must be positive"):
        kafka_module.topic_create("events", num_partitions=0)
