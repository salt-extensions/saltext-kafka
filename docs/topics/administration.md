# Kafka administration

Install the extension and `kafka-python` on the minion that can reach
the brokers. Connection and authentication options are passed in
`client_config`. Put passwords and private key passphrases in
protected Pillar data.

```text
{%- set conn = pillar['kafka']['client_config'] %}

events-topic:
  kafka.topic_present:
    - name: events
    - num_partitions: 6
    - replication_factor: 3
    - bootstrap_servers: {{ pillar['kafka']['bootstrap_servers'] }}
    - client_config: {{ conn | json }}

events-reader-acl:
  kafka.acl_present:
    - name: events-reader
    - bootstrap_servers: {{ pillar['kafka']['bootstrap_servers'] }}
    - client_config: {{ conn | json }}
    - binding:
        resource_type: TOPIC
        resource_name: events
        principal: User:reader
        operation: READ
        permission_type: ALLOW
        host: '*'
        pattern_type: LITERAL

service-a-scram:
  kafka.scram_user_present:
    - name: service-a
    - password: {{ pillar['kafka']['service_a_password'] | json }}
    - mechanism: SCRAM-SHA-512
    - bootstrap_servers: {{ pillar['kafka']['bootstrap_servers'] }}
    - client_config: {{ conn | json }}
```

Run `state.apply test=True` first to inspect planned changes. Topic
states create topics and increase partition counts; Kafka cannot
decrease partitions. Topic configuration and replication reassignment
are not reconciled for existing topics. ACL states manage individual
bindings; authorizer behavior and broker authorization apply. ACL
deletion filters can match multiple entries, so use specific values
and inspect changes carefully. SCRAM passwords cannot be read back
from Kafka: the present state creates a missing credential, but does
not detect or rotate a changed password. Call `kafka.scram_user_set`
for an intentional rotation.

Read-only execution examples:

```bash
salt-call kafka.cluster_info bootstrap_servers='kafka-1:9092'
salt-call kafka.topic_list bootstrap_servers='kafka-1:9092'
salt-call kafka.consumer_groups bootstrap_servers='kafka-1:9092'
salt-call kafka.consumer_group_info group_id=payments bootstrap_servers='kafka-1:9092'
salt-call kafka.acl_list bootstrap_servers='kafka-1:9092'
salt-call kafka.scram_users users='["service-a"]' bootstrap_servers='kafka-1:9092'
```

## Ansible coverage and limits

The non-MSK administration behavior covered here is topic
presence/deletion, partition growth, ACL presence/deletion, SCRAM
credential presence/deletion, cluster metadata, topic metadata, ACL
listing, SCRAM credential metadata, and consumer group
listing/description. These correspond to a useful subset of the
`kafka_topic`, `kafka_acl`, `kafka_user`, `kafka_info`, and
`kafka_consumer_group` operations in `ansible-kafka-admin`. Bulk
variants can be expressed as multiple Salt state declarations.
Consumer group mutation and offset reset operations are not included.

Kafka client quotas and consumer lag reporting are not implemented.
The released `kafka-python` API used here does not expose the quota
administration calls documented in its development version. The broker
roles' OS-specific installation, service templates, rolling upgrades,
TLS certificate generation, and inventory-based KRaft quorum
orchestration also need site-specific SLS. Kafka Connect, MirrorMaker,
producers, and consumers are separate workloads and are not managed by
these APIs.
