import json
from core.kafka_client import get_kafka_producer, consume_raw_logs

def push_test_log(topic: str) -> None:
    """Pushes a synthetic syslog event to Kafka to test the pipeline."""
    producer = get_kafka_producer()
    
    test_log = {
        "timestamp": "2026-06-04T18:36:07Z",
        "source_ip": "192.168.1.50",
        "destination_ip": "10.0.0.5",
        "event_type": "Failed Login Attempt",
        "severity": "HIGH",
        "raw_message": "sshd[1432]: Failed password for root from 192.168.1.50 port 54321 ssh2"
    }
    
    encoded_log = json.dumps(test_log).encode("utf-8")
    producer.produce(topic, value=encoded_log)
    producer.flush()
    print("Test log successfully published to Kafka.")

if __name__ == "__main__":
    target_topic = "logs_raw"
    
    push_test_log(topic=target_topic)
    
    consume_raw_logs(topic=target_topic, group_id="sentinel_triage_group")