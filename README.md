# KafkaSparkStreaming

Kafka Spark Streaming application tells how to get data from Kafka in Spark Streaming and processing data and pushing from Spark Streaming to any file in reliable way.

## Quickstart guide

Download latest Apache Kafka distribution and un-tar it.
Start ZooKeeper server:
```sh
./bin/zookeeper-server-start.sh config/zookeeper.properties
```

Start Kafka server:
```sh
./bin/kafka-server-start.sh config/server.properties
```

Create input topic:
```sh
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic messages
```

Start the Kafka producer by running KafkaProducer application:
```sh
sbt "runMain com.analytics.KafkaProducer"
```

Start the Kafka Spark Consumer by running KafkaSparkConsumer application:
```sh
sbt "runMain com.analytics.KafkaSparkConsumer"
```