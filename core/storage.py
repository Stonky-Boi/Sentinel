import json
import datetime
import uuid
from schemas.log_events import IncidentReport
from core.logger import REPORT_DIR, get_logger

logger = get_logger("storage")

def save_report_to_disk(report: IncidentReport, source_ip: str, target_ip: str) -> str:
    """
    Saves an Incident Report to the ~/.sentinel/reports/ directory as a JSON file.
    Returns the unique Report ID.
    """
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    report_id = f"INC-{timestamp}-{uuid.uuid4().hex[:6].upper()}"
    filepath = REPORT_DIR / f"{report_id}.json"
    
    payload = {
        "report_id": report_id,
        "timestamp": datetime.datetime.now().isoformat(),
        "source_ip": source_ip,
        "target_ip": target_ip,
        "incident_title": report.incident_title,
        "severity_level": report.severity_level,
        "executive_summary": report.executive_summary,
        "recommended_actions": report.recommended_actions
    }
    
    try:
        with open(filepath, "w") as f:
            json.dump(payload, f, indent=4)
        logger.info(f"Successfully saved incident report to disk: {report_id}")
        return report_id
    except Exception as e:
        logger.error(f"Failed to save incident report {report_id}. Error: {e}")
        return "ERROR"