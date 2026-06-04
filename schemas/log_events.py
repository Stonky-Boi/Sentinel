from pydantic import BaseModel
from typing import Optional

class NetworkLog(BaseModel):
    timestamp: str
    source_ip: str
    destination_ip: str
    event_type: str
    severity: str
    raw_message: str
    user_agent: Optional[str] = None

class TriageDecision(BaseModel):
    is_anomalous: bool
    confidence_score: float
    reason: str