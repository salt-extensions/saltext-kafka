import pytest

pytestmark = [
    pytest.mark.requires_salt_modules("kafka.example_function"),
]


@pytest.fixture
def kafka(modules):
    return modules.kafka


def test_replace_this_with_something_meaningful(kafka):
    echo_str = "Echoed!"
    res = kafka.example_function(echo_str)
    assert res == echo_str
