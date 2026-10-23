"""Execution functions for Apache Kafka administration."""

from __future__ import annotations

import importlib.util
from enum import Enum

__virtualname__ = "kafka"


def __virtual__():
    """Load this module when kafka-python is installed."""
    if importlib.util.find_spec("kafka") is None:
        return False, "The kafka-python package is required"
    return __virtualname__


def _client(bootstrap_servers, client_config=None):
    """Create an AdminClient. Kept private so callers always close it."""
    admin_client = getattr(importlib.import_module("kafka"), "KafkaAdminClient")
    config = dict(client_config or {})
    config["bootstrap_servers"] = bootstrap_servers
    return admin_client(**config)


def _kafka_attribute(module_name, attribute_name):
    """Load an optional kafka-python API without importing it at module load time."""
    return getattr(importlib.import_module(module_name), attribute_name)


def _plain(value):
    """Convert kafka-python response objects into Salt-serializable values."""
    if isinstance(value, Enum):
        return value.name
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {str(_plain(key)): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_plain(item) for item in value]
    if hasattr(value, "_asdict"):
        return _plain(value._asdict())
    if hasattr(value, "__dict__"):
        return {key: _plain(item) for key, item in vars(value).items() if not key.startswith("_")}
    return str(value)


def _request(bootstrap_servers, client_config, method, *args, **kwargs):
    admin = _client(bootstrap_servers, client_config)
    try:
        return _plain(getattr(admin, method)(*args, **kwargs))
    finally:
        admin.close()


def cluster_info(bootstrap_servers="localhost:9092", client_config=None):
    """
    Return cluster metadata, including the cluster ID, controller, and brokers.

    ``client_config`` accepts kafka-python ``KafkaAdminClient`` options, such as
    ``security_protocol``, TLS paths, SASL settings, and request timeouts. Store
    credentials in protected Pillar data. This function is read-only.

    CLI Example:

    .. code-block:: bash

        salt-call kafka.cluster_info bootstrap_servers='broker1:9092,broker2:9092'
    """
    return _request(bootstrap_servers, client_config, "describe_cluster")


def topic_list(bootstrap_servers="localhost:9092", client_config=None):
    """
    Return topic metadata for every topic in the cluster.

    ``client_config`` accepts kafka-python ``KafkaAdminClient`` options. The
    response is JSON-serializable and contains partition and replica metadata.
    This function is read-only.

    CLI Example:

    .. code-block:: bash

        salt-call kafka.topic_list bootstrap_servers='broker1:9092'
    """
    return _request(bootstrap_servers, client_config, "describe_topics")


def topic_info(name, bootstrap_servers="localhost:9092", client_config=None):
    """
    Return metadata for one topic, or ``None`` when it does not exist.

    ``client_config`` accepts kafka-python ``KafkaAdminClient`` options. This
    function is read-only.

    CLI Example:

    .. code-block:: bash

        salt-call kafka.topic_info name=events bootstrap_servers='broker1:9092'
    """
    topics = _request(bootstrap_servers, client_config, "describe_topics")
    for topic in topics:
        if topic.get("topic") == name or topic.get("name") == name:
            return topic
    return None


def topic_create(
    name,
    num_partitions=1,
    replication_factor=1,
    configs=None,
    bootstrap_servers="localhost:9092",
    client_config=None,
):
    """
    Create a Kafka topic and return the broker response.

    ``num_partitions`` and ``replication_factor`` must be positive integers.
    ``configs`` maps topic configuration names to values. Existing topics are
    not changed by this imperative function; use ``kafka.topic_present`` for
    reconciliation. ``client_config`` accepts kafka-python client options.

    CLI Example:

    .. code-block:: bash

        salt-call kafka.topic_create events num_partitions=3 replication_factor=2
    """
    if int(num_partitions) < 1 or int(replication_factor) < 1:
        raise ValueError("num_partitions and replication_factor must be positive")
    spec = {
        "num_partitions": int(num_partitions),
        "replication_factor": int(replication_factor),
        "configs": dict(configs or {}),
    }
    return _request(
        bootstrap_servers,
        client_config,
        "create_topics",
        {name: spec},
        raise_errors=False,
    )


def topic_delete(name, bootstrap_servers="localhost:9092", client_config=None):
    """
    Delete a topic from the Kafka cluster.

    Deleting a topic also deletes its data and is irreversible. The function
    returns broker responses and does not check whether the topic exists first.
    ``client_config`` accepts kafka-python client options.

    CLI Example:

    .. code-block:: bash

        salt-call kafka.topic_delete events bootstrap_servers='broker1:9092'
    """
    return _request(
        bootstrap_servers,
        client_config,
        "delete_topics",
        [name],
        raise_errors=False,
    )


def topic_partitions_set(
    name,
    num_partitions,
    bootstrap_servers="localhost:9092",
    client_config=None,
):
    """
    Increase a topic's partition count to ``num_partitions``.

    Kafka cannot reduce the partition count. The function raises ``ValueError``
    when the requested count is lower than the current count, and does nothing
    when it already matches. ``client_config`` accepts kafka-python client
    options.

    CLI Example:

    .. code-block:: bash

        salt-call kafka.topic_partitions_set events 8
    """
    info = topic_info(name, bootstrap_servers, client_config)
    if info is None:
        raise ValueError(f"Topic {name!r} does not exist")
    current = len(info.get("partitions", []))
    requested = int(num_partitions)
    if requested < current:
        raise ValueError("Kafka topic partition counts cannot be decreased")
    if requested == current:
        return {name: "already at requested partition count"}
    return _request(
        bootstrap_servers,
        client_config,
        "create_partitions",
        {name: requested},
    )


def consumer_groups(bootstrap_servers="localhost:9092", client_config=None):
    """
    Return consumer group metadata from the cluster.

    ``client_config`` accepts kafka-python ``KafkaAdminClient`` options. This
    function is read-only; group membership and offsets may change while the
    request is running.

    CLI Example:

    .. code-block:: bash

        salt-call kafka.consumer_groups bootstrap_servers='broker1:9092'
    """
    return _request(bootstrap_servers, client_config, "list_groups")


def consumer_group_info(group_id, bootstrap_servers="localhost:9092", client_config=None):
    """
    Return members and state for a consumer group.

    ``client_config`` accepts kafka-python ``KafkaAdminClient`` options. This
    function is read-only.

    CLI Example:

    .. code-block:: bash

        salt-call kafka.consumer_group_info group_id=payments
    """
    return _request(
        bootstrap_servers,
        client_config,
        "describe_groups",
        [group_id],
    )


def acl_list(bootstrap_servers="localhost:9092", client_config=None):
    """
    Return all Kafka ACL bindings.

    The broker must have an authorizer enabled. ``client_config`` accepts
    kafka-python ``KafkaAdminClient`` options. This function is read-only.

    CLI Example:

    .. code-block:: bash

        salt-call kafka.acl_list bootstrap_servers='broker1:9092'
    """
    acl_filter_cls = _kafka_attribute("kafka.admin.acl_resource", "ACLFilter")
    operation_enum = _kafka_attribute("kafka.admin.acl_resource", "ACLOperation")
    permission_enum = _kafka_attribute("kafka.admin.acl_resource", "ACLPermissionType")
    pattern_type_enum = _kafka_attribute("kafka.admin.acl_resource", "ACLResourcePatternType")
    resource_type_enum = _kafka_attribute("kafka.admin.acl_resource", "ResourceType")

    acl_filter = acl_filter_cls(
        resource_type=resource_type_enum.ANY,
        resource_name=None,
        pattern_type=pattern_type_enum.ANY,
        principal=None,
        host=None,
        operation=operation_enum.ANY,
        permission_type=permission_enum.ANY,
    )
    admin = _client(bootstrap_servers, client_config)
    try:
        acls, error = admin.describe_acls(acl_filter)
        if error is not None and getattr(error, "errno", 0) != 0:
            raise RuntimeError(str(error))
        return _plain(acls)
    finally:
        admin.close()


def acl_create(binding, bootstrap_servers="localhost:9092", client_config=None):
    """
    Add one Kafka ACL binding.

    ``binding`` is a mapping with ``resource_type``, ``resource_name``,
    ``principal``, ``operation``, ``permission_type``, and optional ``host``
    and ``pattern_type``. Enum values use Kafka names such as ``TOPIC``,
    ``READ``, ``ALLOW``, and ``LITERAL``. Duplicate ACLs are harmless. The
    broker must have an authorizer enabled. This changes cluster access policy.

    CLI Example:

    .. code-block:: bash

        salt-call kafka.acl_create binding='{"resource_type":"TOPIC","resource_name":"events","principal":"User:reader","operation":"READ","permission_type":"ALLOW"}'
    """
    acl_cls = _kafka_attribute("kafka.admin.acl_resource", "ACL")
    operation_enum = _kafka_attribute("kafka.admin.acl_resource", "ACLOperation")
    permission_enum = _kafka_attribute("kafka.admin.acl_resource", "ACLPermissionType")
    pattern_type_enum = _kafka_attribute("kafka.admin.acl_resource", "ACLResourcePatternType")
    resource_pattern_cls = _kafka_attribute("kafka.admin.acl_resource", "ResourcePattern")
    resource_type_enum = _kafka_attribute("kafka.admin.acl_resource", "ResourceType")

    resource_type = resource_type_enum[binding["resource_type"].upper()]
    pattern_type = pattern_type_enum[binding.get("pattern_type", "LITERAL").upper()]
    acl = acl_cls(
        principal=binding["principal"],
        host=binding.get("host", "*"),
        operation=operation_enum[binding["operation"].upper()],
        permission_type=permission_enum[binding["permission_type"].upper()],
        resource_pattern=resource_pattern_cls(
            resource_type,
            binding["resource_name"],
            pattern_type,
        ),
    )
    return _request(bootstrap_servers, client_config, "create_acls", [acl])


def acl_delete(binding, bootstrap_servers="localhost:9092", client_config=None):
    """
    Delete ACL entries matching a binding filter.

    Required mapping fields are ``resource_type``, ``resource_name``,
    ``principal``, and ``operation``. Optional ``host``, ``permission_type``,
    and ``pattern_type`` further constrain the filter. This changes cluster
    access policy; review wildcard values carefully. The broker must have an
    authorizer enabled.

    CLI Example:

    .. code-block:: bash

        salt-call kafka.acl_delete binding='{"resource_type":"TOPIC","resource_name":"events","principal":"User:reader","operation":"READ"}'
    """
    acl_filter_cls = _kafka_attribute("kafka.admin.acl_resource", "ACLFilter")
    operation_enum = _kafka_attribute("kafka.admin.acl_resource", "ACLOperation")
    permission_enum = _kafka_attribute("kafka.admin.acl_resource", "ACLPermissionType")
    pattern_type_enum = _kafka_attribute("kafka.admin.acl_resource", "ACLResourcePatternType")
    resource_type_enum = _kafka_attribute("kafka.admin.acl_resource", "ResourceType")

    acl_filter = acl_filter_cls(
        resource_type=resource_type_enum[binding["resource_type"].upper()],
        resource_name=binding["resource_name"],
        pattern_type=pattern_type_enum[binding.get("pattern_type", "LITERAL").upper()],
        principal=binding["principal"],
        host=binding.get("host"),
        operation=operation_enum[binding["operation"].upper()],
        permission_type=(
            permission_enum[binding["permission_type"].upper()]
            if binding.get("permission_type")
            else permission_enum.ANY
        ),
    )
    return _request(bootstrap_servers, client_config, "delete_acls", [acl_filter])


def scram_user_set(
    username,
    password,
    mechanism="SCRAM-SHA-512",
    iterations=4096,
    bootstrap_servers="localhost:9092",
    client_config=None,
):
    """
    Create or replace a Kafka user's SCRAM credential.

    ``mechanism`` must be ``SCRAM-SHA-256`` or ``SCRAM-SHA-512``. ``iterations``
    is the PBKDF2 iteration count. Kafka does not allow reading a credential's
    password, so this imperative function always submits the requested value;
    use ``kafka.scram_user_present`` for safe presence-only reconciliation.
    Requires a broker that supports AlterUserScramCredentials (Kafka 2.7+).
    The password is sent to the broker and is never included in the return.

    CLI Example:

    .. code-block:: bash

        salt-call kafka.scram_user_set service-a password='{{ pillar["kafka"]["service_a_password"] }}'
    """
    scram_mechanism = _kafka_attribute("kafka.admin", "ScramMechanism")
    upsertion_cls = _kafka_attribute("kafka.admin", "UserScramCredentialUpsertion")
    scram = scram_mechanism[mechanism.replace("-", "_")]
    upsertion = upsertion_cls(username, scram, password, int(iterations))
    return _request(
        bootstrap_servers,
        client_config,
        "alter_user_scram_credentials",
        [upsertion],
    )


def scram_user_delete(
    username,
    mechanism="SCRAM-SHA-512",
    bootstrap_servers="localhost:9092",
    client_config=None,
):
    """
    Remove a Kafka user's SCRAM credential for one mechanism.

    ``mechanism`` must be ``SCRAM-SHA-256`` or ``SCRAM-SHA-512``. This removes
    a credential from the broker and may prevent clients using it from
    authenticating. Requires Kafka 2.7 or newer.

    CLI Example:

    .. code-block:: bash

        salt-call kafka.scram_user_delete service-a mechanism=SCRAM-SHA-512
    """
    scram_mechanism = _kafka_attribute("kafka.admin", "ScramMechanism")
    deletion_cls = _kafka_attribute("kafka.admin", "UserScramCredentialDeletion")
    deletion = deletion_cls(username, scram_mechanism[mechanism.replace("-", "_")])
    return _request(
        bootstrap_servers,
        client_config,
        "alter_user_scram_credentials",
        [deletion],
    )


def scram_users(users=None, bootstrap_servers="localhost:9092", client_config=None):
    """
    Return SCRAM credential metadata for one or more Kafka users.

    Passwords are never returned by Kafka. ``users`` may be a list of user
    names; ``None`` requests all configured SCRAM users. ``client_config``
    accepts kafka-python ``KafkaAdminClient`` options. Requires broker support
    for the SCRAM credential administration API (Kafka 2.7 or newer).

    CLI Example:

    .. code-block:: bash

        salt-call kafka.scram_users users='["service-a"]'
    """
    return _request(
        bootstrap_servers,
        client_config,
        "describe_user_scram_credentials",
        users=users,
    )
