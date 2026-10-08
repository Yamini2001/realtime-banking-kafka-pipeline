# Real-Time Bank Transaction Pipeline: Apache Kafka & Kafka Connect

A hands-on Data Engineering project demonstrating a **Real-Time Bank Transaction Pipeline** using **Apache Kafka (KRaft mode)** and **Kafka Connect**. The project is split into two architectural pathways: an application-driven **Producer/Consumer pipeline** for real-time fraud detection, and a configuration-driven **Kafka Connect Source/Sink pipeline** for automated transaction archiving.

---

## 📌 Project Architecture

The pipeline processes event streams through two distinct workflows:

```text
                    [ PART A: Event Streaming & Consumer Groups ]

   Payment Application (Producer) ---> Kafka Topic: `payments` (3 Partitions) ---> Fraud Detection Service (Consumer Group)
   

                    [ PART B: Configuration-Driven Kafka Connect ]

   Core Banking Log File ---> FileStreamSource Connector ---> Kafka Topic: `bank-transactions` ---> FileStreamSink Connector ---> Archive File
```

### 1️⃣ Part A: Core Event Streaming
* **Payment Application (Producer):** Streams continuous key-value (`Account ID | Transaction JSON`) JSON messages into Kafka.
* **Topic & Partitioning Strategy:** The `payments` topic is configured with **3 partitions**. Kafka hashes the Account ID key to route transactions from the same account to the identical partition, ensuring strict ordering guarantees.
* **Fraud Detection Service (Consumer):** Partitions are distributed and parallelized across a dedicated consumer group (`fraud-service`).

### 2️⃣ Part B: No-Code Data Integration (Kafka Connect)
* **Source Connector:** A `FileStreamSource` connector acts as a persistent agent monitoring a living core banking log file (`transactions.log`), immediately publishing new lines to Kafka.
* **Sink Connector:** A `FileStreamSink` connector consumes events from the `bank-transactions` topic and archives them to an external text file (`transactions_archive.txt`).
* **Key Takeaway:** Moves data seamlessly between external systems and Kafka via `.properties` configuration files without writing custom application code.

---

## 🛠️ Key Concepts Demonstrated

| Component | Architecture & Implementation Details |
| :--- | :--- |
| **Event Streaming** | Decoupled, asynchronous producer-consumer event architecture using key-value parsing. |
| **Partition Routing** | Key-based partition routing ensuring message-ordering guarantees for specific financial accounts. |
| **Consumer Tracking** | Monitoring consumers using `kafka-consumer-groups.sh` to measure processing velocity and verify a `LAG = 0` state. |
| **Kafka Connect** | Implementing configuration-driven ingestion and egress pipelines over Standalone framework modes. |
| **REST API Monitoring** | Querying the internal Kafka Connect REST interface engine to audit connector health states dynamically. |

---

## 📂 Project Structure

```text
realtime-bank-kafka-pipeline/
│
├── config/                  # Standalone property extensions
│   ├── file-source.properties
│   └── file-sink.properties
│
├── data/                    # Ingestion and Egress target logs
│   ├── source/
│   │   └── transactions.log
│   └── sink/
│       └── transactions_archive.txt
│
└── README.md
```

---

## 🚀 Step-by-Step Execution Guide

### Prerequisite Environment
* Apache Kafka **3.7.0** running on **KRaft architecture**.
* **Java 8 / OpenJDK 1.8** or compatible engine environment.
* Linux bash terminal environment or Windows Subsystem for Linux (WSL).

---

### Step 1: Start the KRaft Kafka Server
Generate a storage UUID cluster identifier, format the log directory mapping, and start the broker engine:

```bash
# Generate UUID
bin/kafka-storage.sh random-uuid

# Format storage directories (replace <UUID> with your generated token)
bin/kafka-storage.sh format -t <YOUR_GENERATED_UUID> -c config/kraft/server.properties

# Boot the Cluster Broker
bin/kafka-server-start.sh config/kraft/server.properties
```

---

### Step 2: Part A Setup (Producer, Consumer & Topics)

Open a new terminal session to provision the partitioned topic framework:
```bash
bin/kafka-topics.sh --create --topic payments --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

Start the interactive standard consumer loop tied to the `fraud-service` consumer group flag:
```bash
bin/kafka-console-consumer.sh --topic payments --bootstrap-server localhost:9092 --group fraud-service --from-beginning --property print.key=true --property print.partition=true
```

Open another terminal to fire up the console producer using standard pipe-delimiters for key parsing:
```bash
bin/kafka-console-producer.sh --topic payments --bootstrap-server localhost:9092 --property parse.key=true --property key.separator="|"
```
*Paste your structural key-value events into the producer line terminal prompt:*
```text
acc101|{"txn_id":1,"amount":2500,"type":"DEBIT"}
acc102|{"txn_id":2,"amount":99000,"type":"CREDIT"}
acc101|{"txn_id":3,"amount":120,"type":"DEBIT"}
```

---

### Step 3: Part B Setup (Kafka Connect Integration)

Create local execution landing directories for file streaming operations:
```bash
mkdir -p ~/connect-demo/source ~/connect-demo/sink
```

Populate the core banking baseline records using standard log append techniques:
```bash
nano ~/connect-demo/source/transactions.log
```
*Add baseline JSON payload configurations:*
```json
{"txn_id":101,"account":"acc201","amount":1500,"city":"Bengaluru"}
{"txn_id":102,"account":"acc202","amount":32000,"city":"Mumbai"}
```

Configure the internal system property mappings inside `config/connect-standalone.properties` to locate the standard file connector `.jar` packages:
```properties
plugin.path=/home/<YOUR_USER>/kafka_2.13-3.7.0/libs/connect-file-3.7.0.jar
```

Provision the `bank-transactions` internal broker cluster topic destination:
```bash
bin/kafka-topics.sh --create --topic bank-transactions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Launch the Standalone Connect engine runtime passing in worker environment structures along with the specific Source and Sink parameters:
```bash
bin/connect-standalone.sh config/connect-standalone.properties ~/connect-demo/file-source.properties ~/connect-demo/file-sink.properties
```

---

### Step 4: Verify Real-Time Streaming and Egress Status
In a separate validation shell terminal, review the generated sink archive file layout contents:
```bash
cat ~/connect-demo/sink/transactions_archive.txt
```

Stream live records directly into the active monitoring source file to verify dynamic replication:
```bash
echo '{"txn_id":104,"account":"acc204","amount":88000,"city":"Chennai"}' >> ~/connect-demo/source/transactions.log
sleep 2
cat ~/connect-demo/sink/transactions_archive.txt
```

Verify your active pipeline connection bindings using the built-in REST endpoint hooks:
```bash
curl -s localhost:8083/connectors
curl -s localhost:8083/connectors/txn-file-source/status
```

---

## 📋 Comprehensive Interview Reference

* **Kafka Connect vs Application Clients:** While traditional Producers and Consumers rely on manual codebase creation (Python/Java) to manage polling loops, offsets, and connection retries, Kafka Connect leverages configuration files to map external files or databases directly into Kafka with no custom code.
* **Source vs Sink Components:** Inbound pipelines flowing out of an external source (such as `File -> Kafka`) rely on **Source Connectors**. Outbound analytics pipelines transitioning out of Kafka partitions into a data lake or target database (such as `Kafka -> File/S3/Snowflake`) run on **Sink Connectors**.
* **Architectural Decoupling:** In real-world enterprise architectures, Kafka acts as a central streaming platform. One transaction log stream can be consumed by multiple separate downstream components (Fraud Services, Analytics engines, or Reporting databases) without adding overhead to the core banking application.
