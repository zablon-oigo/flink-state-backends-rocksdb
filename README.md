## Real-Time Customer Loyalty Program with Apache Flink

![Java](https://img.shields.io/badge/Java-17+-orange?logo=java&logoColor=white)
![Maven](https://img.shields.io/badge/Maven-3.8+-C71A36?logo=apache-maven&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Distributed%20Streaming-231F20?logo=apache-kafka&logoColor=white)
![Apache Flink](https://img.shields.io/badge/Apache%20Flink-Stream%20Processing-E6526F?logo=apacheflink&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?logo=docker&logoColor=white)
![RocksDB](https://img.shields.io/badge/RocksDB-State%20Backend-2C8EBB)
![Checkpointing](https://img.shields.io/badge/Flink-Checkpointing-E6526F)
![Prometheus](https://img.shields.io/badge/Prometheus-Monitoring-E6522C?logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-Visualization-F46800?logo=grafana&logoColor=white)




This project demonstrates how to build a real-time customer loyalty program using Apache Flink and Apache Kafka.

The streaming application continuously processes purchase events, identifies returning customers based on their transaction history, and filters customers who qualify for loyalty rewards.

#### Architecture Diagram
<img width="996" height="331" alt="gmart excalidraw" src="https://github.com/user-attachments/assets/7767a4db-8922-4d9e-9755-c4849da84ce4" />



#### Build the Project

```sh
mvn clean package
```

#### Inspect the Generated JAR

To verify that the application classes were packaged correctly:

```sh
jar tf target/flink-filter.jar
```

Copy the JAR to the Flink JobManager

```sh
docker cp target/original-flink-filter.jar  jobmanager:/opt/flink/usrlib
```

Access the JobManager Container

```sh
docker compose exec -it jobmanager bash
```

#### Run the Flink Job

```sh
cd /opt/flink/usrlib

flink run \
  -c kmart.HighValuePurchaseFilter \
  flink-filter.jar

```


#### Cancel a Running Job

List running jobs:

```sh
flink list
```

Cancel a job:

```sh
flink cancel <job_id>
```
