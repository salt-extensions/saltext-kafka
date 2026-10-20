"""State functions for Kafka topics, ACLs, and SCRAM credentials."""

__virtualname__ = "kafka"


def __virtual__():
    """Load when the Kafka execution module is available."""
    if "kafka.topic_info" not in __salt__:
        return False, "The 'kafka' execution module is not available"
    return __virtualname__


def _ret(name):
    return {"name": name, "changes": {}, "result": True, "comment": ""}


def _would_change(ret, test, comment):
    ret["result"] = None if test else True
    ret["comment"] = comment
    if not test:
        ret["changes"] = {"new": comment}


def _matches_acl(item, binding, exact=True):
    pattern = item.get("resource_pattern", {})
    expected = {
        "principal": binding["principal"],
        "operation": binding["operation"],
    }
    if exact or binding.get("host") is not None:
        expected["host"] = binding.get("host", "*")
    if exact or binding.get("permission_type") is not None:
        expected["permission_type"] = binding["permission_type"]
    if any(item.get(key) != value for key, value in expected.items()):
        return False
    return (
        pattern.get("resource_type") == binding["resource_type"]
        and pattern.get("name", pattern.get("resource_name")) == binding["resource_name"]
        and pattern.get("pattern_type", "LITERAL") == binding.get("pattern_type", "LITERAL")
    )


def topic_present(
    name,
    num_partitions=1,
    replication_factor=1,
    configs=None,
    bootstrap_servers="localhost:9092",
    client_config=None,
):
    """
    Ensure a Kafka topic exists with at least the requested partition count.

    ``num_partitions`` and ``replication_factor`` are positive integers.
    ``configs`` maps topic config keys to values and is applied on creation;
    existing topic configs and replication assignments are not reconciled.
    Existing topics can have their partition count increased, never decreased.
    ``bootstrap_servers`` defaults to ``localhost:9092``. ``client_config``
    accepts kafka-python client options, including TLS and SASL settings.

    Salt test mode reports a planned change without contacting a mutating API.
    Requires a reachable Kafka broker and ``kafka-python``. Broker protocol
    support and authorization determine which operations succeed.

    CLI Example:

    .. code-block:: yaml

        ensure-events-topic:
          kafka.topic_present:
            - name: events
            - num_partitions: 6
            - replication_factor: 3
            - bootstrap_servers: kafka-1:9092,kafka-2:9092
    """
    ret = _ret(name)
    test = __opts__.get("test", False)
    info = __salt__["kafka.topic_info"](name, bootstrap_servers, client_config)
    if info is None:
        _would_change(ret, test, f"Topic {name!r} would be created")
        if not test:
            response = __salt__["kafka.topic_create"](
                name,
                num_partitions,
                replication_factor,
                configs,
                bootstrap_servers,
                client_config,
            )
            ret["changes"] = {"created": response}
            ret["comment"] = f"Created topic {name!r}"
        return ret

    current = len(info.get("partitions", []))
    desired = int(num_partitions)
    if desired < current:
        ret["result"] = False
        ret["comment"] = (
            f"Topic {name!r} has {current} partitions; Kafka cannot reduce it to {desired}"
        )
        return ret
    if desired == current:
        ret["comment"] = f"Topic {name!r} already has {current} partitions"
        return ret
    _would_change(
        ret, test, f"Topic {name!r} partitions would increase from {current} to {desired}"
    )
    if not test:
        response = __salt__["kafka.topic_partitions_set"](
            name, desired, bootstrap_servers, client_config
        )
        ret["changes"] = {"partitions": {"old": current, "new": desired}, "response": response}
        ret["comment"] = f"Increased topic {name!r} partitions from {current} to {desired}"
    return ret


def topic_absent(name, bootstrap_servers="localhost:9092", client_config=None):
    """
    Ensure a Kafka topic is absent.

    Deleting a topic removes its data and is irreversible. ``bootstrap_servers``
    defaults to ``localhost:9092``; ``client_config`` accepts kafka-python
    client options. Salt test mode reports a planned deletion without sending
    a delete request. Kafka topic deletion must be enabled on the broker.

    CLI Example:

    .. code-block:: yaml

        remove-old-topic:
          kafka.topic_absent:
            - name: old-events
            - bootstrap_servers: kafka-1:9092
    """
    ret = _ret(name)
    if __salt__["kafka.topic_info"](name, bootstrap_servers, client_config) is None:
        ret["comment"] = f"Topic {name!r} is already absent"
        return ret
    test = __opts__.get("test", False)
    _would_change(ret, test, f"Topic {name!r} would be deleted")
    if not test:
        response = __salt__["kafka.topic_delete"](name, bootstrap_servers, client_config)
        ret["changes"] = {"deleted": response}
        ret["comment"] = f"Deleted topic {name!r}"
    return ret


def acl_present(
    name,
    binding,
    bootstrap_servers="localhost:9092",
    client_config=None,
):
    """
    Ensure one Kafka ACL binding exists.

    ``binding`` requires ``resource_type``, ``resource_name``, ``principal``,
    ``operation``, and ``permission_type``; ``host`` defaults to ``*`` and
    ``pattern_type`` defaults to ``LITERAL``. Use Kafka enum names. The broker
    must have an authorizer enabled. Salt test mode reports a planned ACL
    addition without changing broker policy. Wildcard ACLs have broad effects.

    CLI Example:

    .. code-block:: yaml

        allow-events-reader:
          kafka.acl_present:
            - name: allow-events-reader
            - binding:
                resource_type: TOPIC
                resource_name: events
                principal: User:reader
                operation: READ
                permission_type: ALLOW
            - bootstrap_servers: kafka-1:9092
    """
    ret = _ret(name)
    # Querying the exact filter distinguishes this binding from unrelated ACLs.
    current = __salt__["kafka.acl_list"](bootstrap_servers, client_config)
    if any(_matches_acl(item, binding) for item in current):
        ret["comment"] = "The requested ACL binding is already present"
        return ret
    test = __opts__.get("test", False)
    _would_change(ret, test, "The requested ACL binding would be added")
    if not test:
        response = __salt__["kafka.acl_create"](binding, bootstrap_servers, client_config)
        ret["changes"] = {"created": response}
        ret["comment"] = "Added the requested ACL binding"
    return ret


def acl_absent(
    name,
    binding,
    bootstrap_servers="localhost:9092",
    client_config=None,
):
    """
    Ensure matching Kafka ACL entries are absent.

    ``binding`` uses ``resource_type``, ``resource_name``, ``principal``, and
    ``operation``; optional ``host``, ``permission_type``, and ``pattern_type``
    narrow the deletion filter. Wildcard or broad filters can remove multiple
    ACLs. Salt test mode reports a planned removal. Requires an enabled broker
    authorizer and adequate cluster permissions.

    CLI Example:

    .. code-block:: yaml

        remove-events-reader:
          kafka.acl_absent:
            - name: remove-events-reader
            - binding:
                resource_type: TOPIC
                resource_name: events
                principal: User:reader
                operation: READ
            - bootstrap_servers: kafka-1:9092
    """
    ret = _ret(name)
    current = __salt__["kafka.acl_list"](bootstrap_servers, client_config)
    matches = [item for item in current if _matches_acl(item, binding, exact=False)]
    if not matches:
        ret["comment"] = "The requested ACL binding is already absent"
        return ret
    test = __opts__.get("test", False)
    _would_change(ret, test, "Matching ACL entries would be removed")
    if not test:
        response = __salt__["kafka.acl_delete"](binding, bootstrap_servers, client_config)
        ret["changes"] = {"deleted": response}
        ret["comment"] = "Removed matching ACL entries"
    return ret


def scram_user_present(
    name,
    password,
    mechanism="SCRAM-SHA-512",
    iterations=4096,
    bootstrap_servers="localhost:9092",
    client_config=None,
):
    """
    Ensure a SCRAM credential exists for a Kafka user and mechanism.

    ``password`` is required but cannot be compared with an existing broker
    credential because Kafka never returns passwords. Therefore this state
    creates the credential when missing and leaves an existing credential
    unchanged, even if the supplied password differs. Use
    ``kafka.scram_user_set`` for an intentional password rotation. ``mechanism``
    accepts ``SCRAM-SHA-256`` or ``SCRAM-SHA-512`` and ``iterations`` defaults
    to 4096. Test mode reports creation without sending the password. Requires
    Kafka 2.7 or newer and admin authorization.

    CLI Example:

    .. code-block:: yaml

        service-a-scram:
          kafka.scram_user_present:
            - name: service-a
            - password: {{ pillar['kafka']['service_a_password'] }}
            - mechanism: SCRAM-SHA-512
            - bootstrap_servers: kafka-1:9092
    """
    ret = _ret(name)
    mechanism = mechanism.upper()
    existing = __salt__["kafka.scram_users"]([name], bootstrap_servers, client_config)
    credentials = existing.get(name, {}).get("credential_infos", [])
    if any(
        str(item.get("mechanism", "")).replace("_", "-").upper() == mechanism
        for item in credentials
    ):
        ret["comment"] = f"SCRAM credential for {name!r} using {mechanism} is present"
        return ret
    test = __opts__.get("test", False)
    _would_change(ret, test, f"SCRAM credential for {name!r} using {mechanism} would be created")
    if not test:
        response = __salt__["kafka.scram_user_set"](
            name,
            password,
            mechanism,
            iterations,
            bootstrap_servers,
            client_config,
        )
        ret["changes"] = {"credential": {"mechanism": mechanism, "response": response}}
        ret["comment"] = f"Created SCRAM credential for {name!r} using {mechanism}"
    return ret


def scram_user_absent(
    name,
    mechanism="SCRAM-SHA-512",
    bootstrap_servers="localhost:9092",
    client_config=None,
):
    """
    Ensure a Kafka user's SCRAM credential for one mechanism is absent.

    Removing credentials can prevent clients from authenticating. ``mechanism``
    accepts ``SCRAM-SHA-256`` or ``SCRAM-SHA-512``. Test mode reports a planned
    removal without contacting the mutating API. Requires Kafka 2.7 or newer
    and admin authorization.

    CLI Example:

    .. code-block:: yaml

        remove-service-a-scram:
          kafka.scram_user_absent:
            - name: service-a
            - mechanism: SCRAM-SHA-512
            - bootstrap_servers: kafka-1:9092
    """
    ret = _ret(name)
    mechanism = mechanism.upper()
    existing = __salt__["kafka.scram_users"]([name], bootstrap_servers, client_config)
    credentials = existing.get(name, {}).get("credential_infos", [])
    if not any(
        str(item.get("mechanism", "")).replace("_", "-").upper() == mechanism
        for item in credentials
    ):
        ret["comment"] = f"SCRAM credential for {name!r} using {mechanism} is already absent"
        return ret
    test = __opts__.get("test", False)
    _would_change(ret, test, f"SCRAM credential for {name!r} using {mechanism} would be removed")
    if not test:
        response = __salt__["kafka.scram_user_delete"](
            name, mechanism, bootstrap_servers, client_config
        )
        ret["changes"] = {"credential": {"mechanism": mechanism, "response": response}}
        ret["comment"] = f"Removed SCRAM credential for {name!r} using {mechanism}"
    return ret
