import json
import time
from core.kafka_client import get_kafka_producer, consume_raw_logs
from data.test_logs import NETWORK_LOGS

def push_test_logs(topic: str) -> None:
    """Pushes a predefined list of synthetic syslog events to Kafka."""
    producer = get_kafka_producer()
    print(f"Pushing {len(NETWORK_LOGS)} test logs to topic '{topic}'...\n")
    for log in NETWORK_LOGS:
        try:
            encoded_log = json.dumps(log).encode("utf-8")
            producer.produce(topic, value=encoded_log)
            print(f"Produced log: {log['event_type']} from {log['source_ip']}")
            # Simulate network delay between incoming logs
            time.sleep(0.5)
        except Exception as production_error:
            print(f"[ERROR] Failed to produce log. Error: {production_error}")
    producer.flush()
    print("\nAll test logs successfully published to Kafka. Starting Consumer...\n")
    print("-" * 60)

if __name__ == "__main__":
    target_topic = "logs_raw"
    push_test_logs(topic=target_topic)
    consume_raw_logs(topic=target_topic, group_id="sentinel_triage_group")