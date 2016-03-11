In order to run this example, we need to install the followings:

1. Scala 2.10
2. Apache Kafka
3. SnappyData

Please follow the below steps to run the example:

Start Zookeeper:
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```
Start Kafka Broker1:
```
bin/kafka-server-start.sh config/server1.properties
```
Start Kafka Broker2:
```
bin/kafka-server-start.sh config/server2.properties
```
Create a topic “adnetwork-topic”:
```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic adnetwork-topic
```
Start SnappyData cluster: 
```
snappy-commons/build-artifacts/scala-2.10/snappy $ ./sbin/snappy-start-all.sh
```
Please note that you need to comment out following line in snappy-start-all.sh
```
#"$sbin"/snappy-leads.sh start
```
Start generating and publishing logs to Kafka
```
./gradlew loggenerator
```
Start aggregation
```
./gradlew logaggregator
```
