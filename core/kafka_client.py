import json
from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
from schemas.log_events import NetworkLog
from core.qdrant_client import get_qdrant_client, setup_collection, store_log_in_qdrant
from agents.triage_agent import analyze_log_for_anomalies
from agents.retrieval_agent import retrieve_similar_logs
from agents.reasoning_agent import generate_incident_report
from core.logger import get_logger

logger = get_logger("kafka_client")
qdrant = get_qdrant_client()
collection = "network_logs"
setup_collection(client=qdrant, collection_name=collection)

def get_kafka_consumer(group_id: str) -> Consumer:
    """Initializes and returns a Kafka consumer configured for local development."""
    config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "session.timeout.ms": 45000,
    }
    return Consumer(config)

def get_kafka_producer() -> Producer:
    """Initializes and returns a Kafka producer for sending structured logs."""
    config = {
        "bootstrap.servers": "localhost:9092"
    }
    return Producer(config)

dlq_producer = get_kafka_producer()

def send_to_dlq(raw_message: str, error_context: str) -> None:
    """Routes malformed or unprocessable logs to the Dead Letter Queue topic."""
    try:
        dlq_payload = json.dumps({
            "original_message": raw_message,
            "error_reason": error_context
        }).encode("utf-8")
        
        dlq_producer.produce("logs_dead_letter", value=dlq_payload)
        dlq_producer.flush()
        logger.warning(f"Routed malformed message to DLQ. Reason: {error_context}")
    except Exception as dlq_error:
        logger.error(f"CRITICAL: Failed to write to DLQ! Error: {dlq_error}")

def process_log_worker(validated_log: NetworkLog) -> None:
    """Background worker function that handles the heavy LLM inference and Qdrant storage."""
    try:
        store_log_in_qdrant(client=qdrant, collection_name=collection, log=validated_log)
        
        logger.info(f"[{validated_log.source_ip}] Analyzing log for anomalies...")
        triage_result = analyze_log_for_anomalies(log=validated_log)
        
        if triage_result.is_anomalous:
            logger.warning(f"[{validated_log.source_ip}] Anomaly Detected! Confidence: {triage_result.confidence_score}")
            logger.warning(f"[{validated_log.source_ip}] Reason: {triage_result.reason}")

            logger.info(f"[{validated_log.source_ip}] Searching memory for similar past events...")
            historical_context = retrieve_similar_logs(
                client=qdrant,
                collection_name=collection,
                anomalous_log=validated_log,
                limit=2
            )

            if historical_context:
                logger.info(f"[{validated_log.source_ip}] Found {len(historical_context)} similar past event(s).")
                for past_event in historical_context:
                    logger.info(f"[{validated_log.source_ip}] Context -> [{past_event['score']:.2f}] {past_event['event_type']} from {past_event['source_ip']}")
            else:
                logger.info(f"[{validated_log.source_ip}] No similar past events found. This is a novel anomaly.") 

            logger.info(f"[{validated_log.source_ip}] Drafting incident report...")
            report = generate_incident_report(
                log=validated_log,
                triage=triage_result,
                history=historical_context
            )
            logger.error(f"[{validated_log.source_ip}] INCIDENT REPORT: {report.incident_title} | Severity: {report.severity_level} | Summary: {report.executive_summary} | Actions: {', '.join(report.recommended_actions)}")
        else:
            logger.info(f"[{validated_log.source_ip}] Log cleared. Reason: {triage_result.reason}")
            
    except Exception as worker_error:
        logger.error(f"Worker failed to process log from {validated_log.source_ip}. Error: {worker_error}")

def consume_raw_logs(topic: str, group_id: str) -> None:
    """Continuously polls the Kafka topic and dispatches raw logs to background workers."""
    consumer = get_kafka_consumer(group_id=group_id)
    consumer.subscribe([topic])

    logger.info(f"Starting consumer for topic: {topic}. Waiting for logs...")

    # We use 4 concurrent background workers. This allows the main thread to keep polling Kafka instantly.
    with ThreadPoolExecutor(max_workers=4) as executor:
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
                    logger.info(f"Parsed valid log from {validated_log.source_ip}: {validated_log.event_type} - Dispatching to worker.")
                    
                    # Offload the heavy AI work to a background thread
                    executor.submit(process_log_worker, validated_log)
                    
                except json.JSONDecodeError as decode_error:
                    logger.error(f"Failed to decode JSON string. Routing to DLQ.")
                    send_to_dlq(raw_message=raw_data, error_context=f"JSON Decode Error: {str(decode_error)}")
                except ValueError as validation_error:
                    logger.error(f"Log format validation failed. Routing to DLQ.")
                    send_to_dlq(raw_message=raw_data, error_context=f"Schema Validation Error: {str(validation_error)}")

        except KeyboardInterrupt:
            logger.info("Interrupt signal received. Shutting down consumer gracefully...")
        finally:
            consumer.close()