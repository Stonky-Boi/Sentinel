from typing import List, Dict, Any
from qdrant_client import QdrantClient
from schemas.log_events import NetworkLog
from core.qdrant_client import embed_log

def retrieve_similar_logs(
    client: QdrantClient, 
    collection_name: str, 
    anomalous_log: NetworkLog, 
    limit: int = 3
) -> List[Dict[str, Any]]:
    """
    Embeds an anomalous log and queries the vector database for similar past events.
    Returns the payloads (raw log dictionaries) of the closest matches.
    """

    try:
        query_vector = embed_log(log=anomalous_log)
        search_response = client.query_points(
            collection_name=collection_name,
            query=query_vector,
            limit=limit,
            with_payload=True
        )
        similar_logs = []
        for point in search_response.points:
            # We skip identical matches by checking the timestamp or raw message
            if point.payload and point.payload.get("raw_message") != anomalous_log.raw_message:
                similar_logs.append({
                    "score": point.score,
                    "event_type": point.payload.get("event_type"),
                    "source_ip": point.payload.get("source_ip"),
                    "raw_message": point.payload.get("raw_message")
                })       
        return similar_logs
    
    except Exception as retrieval_error:
        print(f"[ERROR] Retrieval Agent failed to query Qdrant. Error: {retrieval_error}")
        return []