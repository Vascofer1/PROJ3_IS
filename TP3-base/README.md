1. Reopen the project in the devcontainer. It uses `.devcontainer/docker-compose-cluster.yml`, which starts 3 Kafka brokers.
2. Run `post_connectors.sh` from the `config` folder.
3. Run the producers (`SalesProducer.java` and `RestockProducer.java`).
4. Run `LibraryStreams.java`.

Kafka bootstrap servers used by the Java apps and Kafka Connect:

```bash
broker1:9092,broker2:9092,broker3:9092
```

Create the main topics with 3 replicas before running the demo:

```bash
kafka-topics.sh --bootstrap-server broker1:9092,broker2:9092,broker3:9092 --create --if-not-exists --topic book-sales --partitions 3 --replication-factor 3 --config min.insync.replicas=2
kafka-topics.sh --bootstrap-server broker1:9092,broker2:9092,broker3:9092 --create --if-not-exists --topic book-restocks --partitions 3 --replication-factor 3 --config min.insync.replicas=2
kafka-topics.sh --bootstrap-server broker1:9092,broker2:9092,broker3:9092 --create --if-not-exists --topic book-statistics --partitions 3 --replication-factor 3 --config min.insync.replicas=2
kafka-topics.sh --bootstrap-server broker1:9092,broker2:9092,broker3:9092 --create --if-not-exists --topic total-statistics --partitions 3 --replication-factor 3 --config min.insync.replicas=2
kafka-topics.sh --bootstrap-server broker1:9092,broker2:9092,broker3:9092 --create --if-not-exists --topic window-statistics --partitions 3 --replication-factor 3 --config min.insync.replicas=2
kafka-topics.sh --bootstrap-server broker1:9092,broker2:9092,broker3:9092 --create --if-not-exists --topic user-statistics --partitions 3 --replication-factor 3 --config min.insync.replicas=2
```

Checks:

```bash
kafka-topics.sh --bootstrap-server broker1:9092,broker2:9092,broker3:9092 --list
kafka-topics.sh --bootstrap-server broker1:9092,broker2:9092,broker3:9092 --describe --topic book-sales
kafka-console-consumer.sh --bootstrap-server broker1:9092,broker2:9092,broker3:9092 --include "book-sales|book-restocks|book-statistics|total-statistics|window-statistics|user-statistics" --from-beginning
```

Fault-tolerance demonstration:

1. Start the producers and `LibraryStreams.java`.
2. Confirm that `book-sales` has replicas on all brokers and at least 2 in-sync replicas:

```bash
kafka-topics.sh --bootstrap-server broker1:9092,broker2:9092,broker3:9092 --describe --topic book-sales
```

3. Stop one broker from the host:

```bash
docker compose -f .devcontainer/docker-compose-cluster.yml stop broker1
```

4. Keep the producers and stream app running. Messages should continue flowing because each topic has 3 replicas and requires only 2 in-sync replicas.
5. Describe the topic again. The stopped broker should disappear from the ISR, while the remaining brokers keep the topic available:

```bash
kafka-topics.sh --bootstrap-server broker2:9092,broker3:9092 --describe --topic book-sales
```

6. Restart the broker and verify that it rejoins the ISR:

```bash
docker compose -f .devcontainer/docker-compose-cluster.yml start broker1
kafka-topics.sh --bootstrap-server broker1:9092,broker2:9092,broker3:9092 --describe --topic book-sales
```
