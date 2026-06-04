import uuid
import ollama
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from schemas.log_events import NetworkLog

def get_qdrant_client() -> QdrantClient:
    """Initializes and returns a connection to the local Qdrant instance."""
    return QdrantClient(url="http://localhost:6333")

def setup_collection(client: QdrantClient, collection_name: str, vector_size: int = 768) -> None:
    """Creates a Qdrant collection if it does not already exist."""
    if not client.collection_exists(collection_name=collection_name):
        client.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
        )
        print(f"Collection '{collection_name}' created successfully in local storage.")
    else:
        print(f"Collection '{collection_name}' already exists.")

def embed_log(log: NetworkLog) -> list[float]:
    """Generates a vector embedding for a network log using Ollama."""
    log_text = (
        f"Event: {log.event_type} | "
        f"Source: {log.source_ip} | "
        f"Dest: {log.destination_ip} | "
        f"Severity: {log.severity} | "
        f"Message: {log.raw_message}"
    )
    
    response = ollama.embeddings(
        model="nomic-embed-text",
        prompt=log_text
    )
    
    return response["embedding"]

def store_log_in_qdrant(client: QdrantClient, collection_name: str, log: NetworkLog) -> None:
    """Embeds and stores a validated network log into Qdrant alongside its metadata."""
    try:
        vector_embedding = embed_log(log=log)
        point_id = str(uuid.uuid4())
        
        point = PointStruct(
            id=point_id,
            vector=vector_embedding,
            payload=log.model_dump()
        )
        
        client.upsert(
            collection_name=collection_name,
            points=[point]
        )
        print(f"[STORED] Log embedded and saved to local Qdrant with ID: {point_id}")
        
    except Exception as storage_error:
        print(f"[ERROR] Failed to embed or store log in Qdrant. Error: {storage_error}")