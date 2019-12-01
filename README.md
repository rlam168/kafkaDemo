# kafkaDemo

### Startup ZooKeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

### Startup Kafka
bin/kafka-server-start.sh config/server.properties


### Create a topic
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test

bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test2


// Show list of topics
bin/kafka-topics.sh --list --bootstrap-server localhost:9092


// Send some messages
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test

bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test2


// Start a consumer
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test2 --from-beginning
