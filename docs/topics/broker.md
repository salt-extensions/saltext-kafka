# Broker installation and configuration

Salt core already provides package, file, archive, and service states.
Use those states for the host-level parts of an Ansible Kafka broker
role. Kafka versions have different KRaft storage-format and
configuration requirements, so keep those values explicit in the SLS
and review Kafka's upgrade guidance before changing a running broker's
version.

This minimal example manages a Kafka properties file and systemd
service after Kafka has been installed under `/opt/kafka` and the
`kafka` account exists:

```yaml
kafka-server-properties:
  file.managed:
    - name: /opt/kafka/config/server.properties
    - user: kafka
    - group: kafka
    - mode: '0640'
    - contents: |
        process.roles=broker,controller
        node.id={{ pillar['kafka']['node_id'] }}
        controller.quorum.voters={{ pillar['kafka']['controller_quorum_voters'] }}
        listeners=PLAINTEXT://:9092,CONTROLLER://:9093
        controller.listener.names=CONTROLLER
        log.dirs=/var/lib/kafka/data

kafka-service:
  service.running:
    - name: kafka
    - enable: true
    - watch:
        - file: kafka-server-properties
```

Use `pkg.installed`, `archive.extracted`, `file.directory`, and
`file.managed` to express the package/download, directories,
ownership, and systemd unit steps that match your target operating
system. The example deliberately leaves the Kafka release URL,
checksum, storage formatting, and cluster ID to the site because they
must be selected for the deployment. For a cluster, set unique node
IDs and a shared cluster ID through Pillar and initialize KRaft
storage once using the Kafka version's documented procedure. For
ZooKeeper mode, set `zookeeper.connect` and omit KRaft-only
properties.

This guidance does not automate rolling upgrades, KRaft storage
migration, certificate issuance, or cluster-wide quorum orchestration.
Test service restart behavior and the broker's upgrade steps in a
staging cluster first.
