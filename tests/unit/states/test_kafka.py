import pytest

import saltext.kafka.states.kafka_mod as kafka_state


@pytest.fixture(autouse=True)
def loader_globals(monkeypatch):
    monkeypatch.setattr(kafka_state, "__salt__", {}, raising=False)
    monkeypatch.setattr(kafka_state, "__opts__", {}, raising=False)


def test_topic_present_creates_missing_topic():
    kafka_state.__salt__.update(
        {
            "kafka.topic_info": lambda *args: None,
            "kafka.topic_create": lambda *args: {"events": "created"},
        }
    )
    result = kafka_state.topic_present("events", num_partitions=3)
    assert result["result"] is True
    assert result["changes"] == {"created": {"events": "created"}}


def test_topic_present_test_mode_does_not_create():
    calls = []
    kafka_state.__opts__["test"] = True
    kafka_state.__salt__.update(
        {
            "kafka.topic_info": lambda *args: None,
            "kafka.topic_create": lambda *args: calls.append(args),
        }
    )
    result = kafka_state.topic_present("events")
    assert result["result"] is None
    assert result["changes"] == {}
    assert calls == []


def test_topic_present_increases_partitions():
    kafka_state.__salt__.update(
        {
            "kafka.topic_info": lambda *args: {"partitions": [{}, {}]},
            "kafka.topic_partitions_set": lambda *args: {"events": "expanded"},
        }
    )
    result = kafka_state.topic_present("events", num_partitions=4)
    assert result["changes"]["partitions"] == {"old": 2, "new": 4}


def test_topic_present_rejects_partition_decrease():
    kafka_state.__salt__["kafka.topic_info"] = lambda *args: {"partitions": [{}, {}, {}]}
    result = kafka_state.topic_present("events", num_partitions=2)
    assert result["result"] is False
    assert "cannot reduce" in result["comment"]


def test_topic_absent_is_idempotent():
    calls = []
    kafka_state.__salt__["kafka.topic_info"] = lambda *args: None
    kafka_state.__salt__["kafka.topic_delete"] = lambda *args: calls.append(args)
    result = kafka_state.topic_absent("events")
    assert result["result"] is True
    assert result["changes"] == {}
    assert calls == []


def test_scram_present_does_not_rotate_existing_credential():
    calls = []
    kafka_state.__salt__.update(
        {
            "kafka.scram_users": lambda *args: {
                "service-a": {
                    "credential_infos": [{"mechanism": "SCRAM_SHA_512", "iterations": 4096}]
                }
            },
            "kafka.scram_user_set": lambda *args: calls.append(args),
        }
    )
    result = kafka_state.scram_user_present("service-a", "new-password")
    assert result["changes"] == {}
    assert calls == []
