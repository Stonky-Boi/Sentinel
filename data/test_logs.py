from typing import List, Dict, Any

NETWORK_LOGS: List[Dict[str, Any]] = [
    # 1. Normal Web Traffic
    {
        "timestamp": "2026-06-04T19:00:00Z",
        "source_ip": "10.0.0.15",
        "destination_ip": "10.0.0.5",
        "event_type": "HTTP GET",
        "severity": "LOW",
        "raw_message": "GET /index.html HTTP/1.1 200 OK",
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
    },
    # 2. SQL Injection Attempt
    {
        "timestamp": "2026-06-04T19:05:22Z",
        "source_ip": "198.51.100.23",
        "destination_ip": "10.0.0.5",
        "event_type": "SQL Injection Attempt",
        "severity": "HIGH",
        "raw_message": "GET /login.php?user=admin' OR '1'='1 HTTP/1.1 403 Forbidden",
        "user_agent": "curl/7.68.0"
    },
    # 3. Routine Background Task / Cron Job
    {
        "timestamp": "2026-06-04T19:08:14Z",
        "source_ip": "127.0.0.1",
        "destination_ip": "127.0.0.1",
        "event_type": "System Task",
        "severity": "INFO",
        "raw_message": "CRON[4523]: (root) CMD ( /usr/local/bin/backup.sh >/dev/null 2>&1)",
        "user_agent": None
    },
    # 4. Reconnaissance / Port Scan
    {
        "timestamp": "2026-06-04T19:10:01Z",
        "source_ip": "203.0.113.45",
        "destination_ip": "10.0.0.22",
        "event_type": "Connection Refused",
        "severity": "MEDIUM",
        "raw_message": "TCP connection refused on port 3306 from 203.0.113.45",
        "user_agent": None
    },
    # 5. Suspicious Internal Lateral Movement
    {
        "timestamp": "2026-06-04T19:15:07Z",
        "source_ip": "10.0.0.44",
        "destination_ip": "10.0.2.1",
        "event_type": "Failed Login Attempt",
        "severity": "HIGH",
        "raw_message": "sshd[8892]: Failed password for root from 10.0.0.44 port 44321 ssh2",
        "user_agent": None
    }
]