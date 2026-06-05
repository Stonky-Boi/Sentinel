import uuid
import ollama
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from schemas.log_events import NetworkLog
from core.logger import get_logger
from core.config import Config

logger = get_logger("qdrant_client")

def get_qdrant_client() -> QdrantClient:
    """Initializes and returns a connection to the local Qdrant instance."""
    return QdrantClient(url=Config["qdrant"]["url"])

def setup_collection(client: QdrantClient, collection_name: str, vector_size: int = Config["qdrant"]["vector_size"]) -> None:
    """Creates a Qdrant collection if it does not already exist."""
    if not client.collection_exists(collection_name=collection_name):
        client.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
        )
        logger.info(f"Collection '{collection_name}' created successfully.")
    else:
        logger.info(f"Collection '{collection_name}' already exists.")

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
        model=Config["llm"]["embedding_model"],
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
        logger.info(f"Log embedded and saved to Qdrant with ID: {point_id}")
        
    except Exception as storage_error:
        logger.error(f"Failed to embed or store log in Qdrant. Error: {storage_error}")