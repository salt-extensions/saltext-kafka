import pytest

pytestmark = [
    pytest.mark.requires_salt_states("kafka.exampled"),
]


@pytest.fixture
def kafka(states):
    return states.kafka


def test_replace_this_with_something_meaningful(kafka):
    echo_str = "Echoed!"
    ret = kafka.exampled(echo_str)
    assert ret.result
    assert not ret.changes
    assert echo_str in ret.comment
