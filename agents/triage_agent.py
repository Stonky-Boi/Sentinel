import json
import ollama
from pydantic import ValidationError
from schemas.log_events import NetworkLog, TriageDecision

def analyze_log_for_anomalies(log: NetworkLog, model_name: str = "qwen2.5") -> TriageDecision:
    """
    Evaluates a network log using a local LLM to determine if it is anomalous.
    Forces the LLM to return a structured JSON response.
    """
    prompt = f"""
    You are a network security triage agent. Analyze the following network log and determine if it represents a security anomaly or malicious behavior.
    
    Respond ONLY in valid JSON format matching this exact structure, with no markdown formatting or extra text:
    {{
        "is_anomalous": true or false,
        "confidence_score": a float between 0.0 and 1.0,
        "reason": "a brief 1-sentence explanation"
    }}
    
    Log Details:
    Event: {log.event_type}
    Source IP: {log.source_ip}
    Destination IP: {log.destination_ip}
    Severity: {log.severity}
    Message: {log.raw_message}
    """
    
    try:
        response = ollama.generate(
            model=model_name,
            prompt=prompt,
            format="json"
        )
        
        response_text = response.get("response", "{}")
        decision_dict = json.loads(response_text)
        
        return TriageDecision(**decision_dict)
        
    except (json.JSONDecodeError, ValidationError) as parsing_error:
        print(f"[WARNING] Triage Agent failed to parse LLM response. Error: {parsing_error}")
        # Fail open: if the LLM hallucinates the JSON format, we assume it's normal to prevent alert fatigue
        return TriageDecision(
            is_anomalous=False, 
            confidence_score=0.0, 
            reason="Failed to parse LLM triage response."
        )
    except Exception as general_error:
        print(f"[ERROR] Triage Agent execution failed. Error: {general_error}")
        raise