import pytest

pytestmark = [
    pytest.mark.requires_salt_modules("kafka.example_function"),
]


def test_replace_this_with_something_meaningful(salt_call_cli):
    echo_str = "Echoed!"
    ret = salt_call_cli.run("kafka.example_function", echo_str)
    assert ret.returncode == 0
    assert ret.data
    assert ret.data == echo_str
