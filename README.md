# SF Crime Statistics with Spark Streaming

## Overview

In this project, a real-world dataset on San Francisco Crime incidents, extracted from Kaggle, are provided for statistical analyses where data is produced by a Kafka server and ingested through Apache Spark Streaming Structure.

## Environment

- Jkd 11.x
- Python 3.7 or above
- Zookeeper
- Kafka
- Scala 2.11.x
- Spark 3.1.x

## How to Run?

### Start Zookeeper

```bash
zookeeper-server-start.sh config/zookeeper.properties
```

### Start Kafka Server

```bash
kafka-server-start.sh config/server.properties
```

### Run Kafka Producer server

```bash
python kafka_server.py
```

### Viewing messages using Kafka consumer console

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9093 --topic "gov.department.police.sf.crime" --from-beginning
```

### Viewing messages by executing python script

```bash
python consumer.py
```

### Submit Spark Streaming Job

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 --master local[*] data_stream.py
```

## Performance Optimization

There are a number of configuration values that can be tuned to achieve optimal and efficient spark stream processing.
Different values of Spark Session configuration have been experimented to see which values we can tune to optimize the latency and throughput of spark tasks.
Each configuration key-value pairs is tested at a time, in 3 mins window running on local machine (standalone mode). The Streaming Query Statistics observed below:

- processingTime: tested with values 5, 15 and 30 seconds. The average throughput rate of 5sec processingTime is 40 records/sec which is 2.5 times more than that of 15sec processingTime (16 records/sec) and 5 times more than that of 30sec processingTime (8 records/sec). The average Operation Duration are 3000ms, 5000ms and 6000ms respectively.

- maxRatePerPartition: tested with values of 50, 200, 500 and 1000. The throughput rate  and Operation Duration are not much different, even though the later seems slightly better.

- spark.sql.shuffle.partitions: is experimented with values of 5, 50 and 100. The throughputgput rates are not significantly different, varying from 36 records/sec to 42 records/sec. However, the Operation Duration of shuffle.partion = 100 is 4000ms which is 1.5 times more than that of shuffle.partition 5 and 50 (2600ms).
