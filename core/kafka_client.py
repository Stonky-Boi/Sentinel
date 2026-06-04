import json
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
from schemas.log_events import NetworkLog
from core.qdrant_client import get_qdrant_client, setup_collection, store_log_in_qdrant
from agents.triage_agent import analyze_log_for_anomalies

# Initialize the client and ensure the collection exists before the consumer loop starts
qdrant = get_qdrant_client()
collection = "network_logs"
setup_collection(client=qdrant, collection_name=collection)

def get_kafka_consumer(group_id: str) -> Consumer:
    """Initializes and returns a Kafka consumer configured for local development."""
    config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": group_id,
        "auto.offset.reset": "earliest"
    }
    return Consumer(config)

def get_kafka_producer() -> Producer:
    """Initializes and returns a Kafka producer for sending structured logs."""
    config = {
        "bootstrap.servers": "localhost:9092"
    }
    return Producer(config)

def consume_raw_logs(topic: str, group_id: str) -> None:
    """Continuously polls the Kafka topic for raw logs and validates them."""
    consumer = get_kafka_consumer(group_id=group_id)
    consumer.subscribe([topic])

    print(f"Starting consumer for topic: {topic}. Waiting for logs...")

    try:
        while True:
            message = consumer.poll(timeout=1.0)
            
            if message is None:
                continue
                
            kafka_error = message.error()
            if kafka_error is not None:
                if kafka_error.code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(kafka_error)

            message_value = message.value()
            if message_value is None:
                continue

            raw_data = message_value.decode("utf-8")
            
            try:
                parsed_dict = json.loads(raw_data)
                validated_log = NetworkLog(**parsed_dict)
                print(f"\n[VALID] Parsed log from {validated_log.source_ip}: {validated_log.event_type}")
                store_log_in_qdrant(client=qdrant, collection_name=collection, log=validated_log)
                
                print("[TRIAGE] Analyzing log for anomalies...")
                triage_result = analyze_log_for_anomalies(log=validated_log)
                if triage_result.is_anomalous:
                    print(f"    [ALERT] Anomaly Detected! Confidence: {triage_result.confidence_score}")
                    print(f"    Reason: {triage_result.reason}")
                    # Future step: Forward to the logs_anomalies Kafka topic for the Retrieval Agent
                else:
                    print(f"    [NORMAL] Log cleared. Reason: {triage_result.reason}")
                
            except json.JSONDecodeError as decode_error:
                print(f"[ERROR] Failed to decode JSON string. Error: {decode_error}")
            except ValueError as validation_error:
                print(f"[ERROR] Log format validation failed. Error: {validation_error}")

    except KeyboardInterrupt:
        print("Interrupt signal received. Shutting down consumer gracefully...")
    finally:
        consumer.close()