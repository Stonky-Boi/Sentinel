# Sentinel

Sentinel is an AI-native, fully local Security Operations Center (SOC) framework. It provides a decoupled, fault-tolerant pipeline that ingests network logs, evaluates them using large language models (LLMs), cross-references historical events using vector memory (RAG), and generates structured, actionable incident reports.

## Core Features

* **Decoupled Event Streaming:** Utilizes Apache Kafka to decouple high-throughput log ingestion from computationally expensive AI inference, preventing bottlenecks and session timeouts.
* **Autonomous AI Triage & Reasoning:** Leverages local LLMs (via Ollama) to parse, evaluate, and generate comprehensive security incident reports without relying on external cloud APIs.
* **Semantic Memory (RAG):** Integrates with Qdrant vector database to embed incoming logs, allowing the system to recall similar historical events and adapt its reasoning based on past context.
* **Fault Tolerance (DLQ):** Implements a native Dead Letter Queue to safely catch, route, and store malformed network packets or JSON parsing failures, ensuring zero data loss.
* **Daemonized Execution:** Runs silently in the background as a detached process managed via PID files, keeping the host terminal free.
* **Interactive CLI & UI:** Features a robust command-line interface powered by the `Rich` library, enabling real-time monitoring and highly readable incident rendering.

## System Architecture

Sentinel follows a strict, asynchronous pipeline designed for resilience and scalability. 

1. **Ingestion Layer (syslog-ng):** Network devices or local systems forward logs via TCP/UDP to `syslog-ng`, which structures the raw data into JSON format and acts as the initial buffer.
2. **Message Broker (Kafka):** Formatted logs are produced to the `logs_raw` Kafka topic. If an ingested log is severely malformed, the pipeline catches the exception and routes the raw payload to the `logs_dead_letter` topic.
3. **Worker Pool:** A lightweight Python consumer rapidly polls Kafka and dispatches valid logs to a `ThreadPoolExecutor`. This allows the main thread to immediately return to listening, handling massive network spikes smoothly.
4. **Triage & Retrieval (Ollama + Qdrant):** The background worker embeds the log and queries Qdrant for similar historical anomalies. Simultaneously, a Triage Agent (LLM) evaluates the log to determine the probability of malicious intent.
5. **Reasoning Engine (Ollama):** If an anomaly is detected, the Reasoning Agent synthesizes the raw log, the triage evaluation, and the historical context into a structured JSON Incident Report.
6. **Storage & Presentation Layer:** Reports are saved to `~/.sentinel/reports/`. The CLI interface can be invoked at any time to parse these JSON files and render them as formatted terminal dashboards.

## Prerequisites

Ensure the following dependencies are installed and running on the host machine:
* Python 3.10 or higher
* Apache Kafka (running on localhost:9092)
* Qdrant (running on localhost:6333)
* Ollama (running locally with `qwen2.5-coder` and `nomic-embed-text` models pulled)
* syslog-ng

## Infrastructure Setup

Sentinel requires several underlying services to handle data streaming, vector memory, and AI inference. Follow the instructions below for your respective operating system. It is recommended to run Kafka, Qdrant, and syslog-ng in separate terminal windows or as background services.

### Option A: macOS (via Homebrew)

**1. Apache Kafka**
Install Kafka, start the server, and create both the primary and Dead Letter Queue (DLQ) topics:
```bash
brew install kafka
kafka-server-start /opt/homebrew/etc/kafka/server.properties

# In a new terminal, create the topics:
kafka-topics --create --topic logs_raw --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics --create --topic logs_dead_letter --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

**2. Qdrant (Vector Database)**
Download the binary, bypass macOS quarantine restrictions, and start the database:

```bash
curl -LO https://github.com/qdrant/qdrant/releases/download/v1.18.1/qdrant-aarch64-apple-darwin.tar.gz
tar -xzf qdrant-aarch64-apple-darwin.tar.gz
xattr -d com.apple.quarantine ./qdrant 
chmod +x ./qdrant
./qdrant
```

**3. Ollama (Local LLM Inference)**
Install the Ollama engine and pull the required reasoning and embedding models:

```bash
brew install --cask ollama
ollama pull qwen2.5-coder
ollama pull nomic-embed-text
```

**4. syslog-ng (Log Ingestion)**
Install the log aggregator and start it using the Sentinel configuration file:

```bash
brew install syslog-ng
syslog-ng -F -f /path/to/Sentinel/syslog_kafka.conf
```

### Option B: Linux (Ubuntu/Debian)

**1. Apache Kafka**
Download the binaries, start the environment, and create the topics:

```bash
wget https://downloads.apache.org/kafka/3.7.0/kafka_2.13-3.7.0.tgz
tar -xzf kafka_2.13-3.7.0.tgz
cd kafka_2.13-3.7.0

# Start Zookeeper and Kafka (run in separate terminals or use systemd)
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

# In a new terminal, create the topics:
bin/kafka-topics.sh --create --topic logs_raw --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic logs_dead_letter --bootstrap-server localhost:9092
```

**2. Qdrant (Vector Database)**
The officially supported method for running Qdrant on Linux is via Docker:

```bash
# Ensure Docker is installed, then run:
docker pull qdrant/qdrant
docker run -p 6333:6333 -p 6334:6334 -v $(pwd)/qdrant_storage:/qdrant/storage:z qdrant/qdrant
```

**3. Ollama (Local LLM Inference)**
Install Ollama via the official install script and pull the models:

```bash
curl -fsSL https://ollama.com/install.sh | sh
ollama pull qwen2.5-coder
ollama pull nomic-embed-text
```

**4. syslog-ng (Log Ingestion)**
Install via the package manager and start it using the Sentinel configuration file:

```bash
sudo apt update
sudo apt install syslog-ng
sudo syslog-ng -F -f /path/to/Sentinel/syslog_kafka.conf
```

## Installation

Sentinel includes an automated installation script that generates an isolated Python environment, creates the required background directories, and registers a global executable wrapper.

1. Clone the repository:
```bash
git clone https://github.com/Stonky-Boi/Sentinel.git
cd Sentinel
```

2. Run the installation script:
```bash
chmod +x install.sh
./install.sh
```

3. Configure your log sources (e.g., firewall, router, or local machine) to forward syslog traffic to `127.0.0.1:5140` (or update `syslog_kafka.conf` as needed).

## Usage Reference

Once installed, Sentinel can be controlled from any directory using the global `sentinel` command.

### Process Management

* `sentinel start` - Spawns the worker pipeline as a detached background daemon.
* `sentinel stop` - Gracefully terminates the background daemon.
* `sentinel status` - Checks the current running state and PID of the daemon.

### Intelligence & Monitoring

* `sentinel monitor` - Opens a live, real-time terminal dashboard that renders new incident reports the moment they are generated.
* `sentinel list` - Prints a chronological table of all generated incident reports and their severity levels.
* `sentinel view <REPORT_ID>` - Renders a specific incident report, detailing the executive summary and recommended action items.

## Configuration

All system parameters, including model selection, Kafka topics, and Qdrant database URLs, are managed via a centralized configuration file located at: `~/.sentinel/config.yaml`

To upgrade the LLM or change the embedding dimensions, update this file and restart the Sentinel daemon. No code modifications are required.
