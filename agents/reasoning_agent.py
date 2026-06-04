import json
import ollama
from typing import List, Dict, Any
from pydantic import ValidationError
from schemas.log_events import NetworkLog, TriageDecision, IncidentReport

def generate_incident_report(
    log: NetworkLog, 
    triage: TriageDecision, 
    history: List[Dict[str, Any]], 
    model_name: str = "qwen2.5-coder"
) -> IncidentReport:
    """
    Synthesizes the live log, triage context, and historical data into an actionable incident report.
    """
    
    history_text = "No similar past events found."
    if history:
        history_text = "\n".join(
            [f"- {item['event_type']} from {item['source_ip']}: {item['raw_message']}" for item in history]
        )

    prompt = f"""
    You are a Senior Security Operations Center (SOC) Analyst. 
    Review the following network anomaly and generate an incident report.
    
    Respond ONLY in valid JSON format matching this exact structure:
    {{
        "incident_title": "A concise title",
        "severity_level": "CRITICAL, HIGH, MEDIUM, or LOW",
        "executive_summary": "A 2-3 sentence summary of what happened and why it matters",
        "recommended_actions": ["Action 1", "Action 2", "Action 3"]
    }}
    
    [CURRENT EVENT]
    Event: {log.event_type}
    Source IP: {log.source_ip}
    Target IP: {log.destination_ip}
    Message: {log.raw_message}
    
    [TRIAGE CONTEXT]
    AI Confidence: {triage.confidence_score}
    AI Reason: {triage.reason}
    
    [HISTORICAL CONTEXT]
    {history_text}
    """
    
    try:
        response = ollama.generate(
            model=model_name,
            prompt=prompt,
            format="json"
        )
        
        response_text = response.get("response", "{}")
        report_dict = json.loads(response_text)
        
        return IncidentReport(**report_dict)
        
    except (json.JSONDecodeError, ValidationError) as parsing_error:
        print(f"[WARNING] Reasoning Agent failed to parse response. Error: {parsing_error}")
        return IncidentReport(
            incident_title="Parsing Error",
            severity_level="LOW",
            executive_summary="The LLM failed to generate a strictly formatted JSON report.",
            recommended_actions=["Review the raw logs manually."]
        )