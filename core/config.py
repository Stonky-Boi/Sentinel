import yaml
from core.logger import SENTINEL_HOME

CONFIG_PATH = SENTINEL_HOME / "config.yaml"

DEFAULT_CONFIG = {
    "kafka": {
        "bootstrap_servers": "localhost:9092",
        "topic_raw": "logs_raw",
        "topic_dlq": "logs_dead_letter",
        "consumer_group": "sentinel_triage_group",
        "max_workers": 4
    },
    "qdrant": {
        "url": "http://localhost:6333",
        "collection": "network_logs",
        "vector_size": 768
    },
    "llm": {
        "embedding_model": "nomic-embed-text",
        "reasoning_model": "qwen2.5-coder",
        "triage_model": "qwen2.5-coder"
    },
    "rag": {
        "retrieval_limit": 2
    }
}

def load_config() -> dict:
    """Loads the user configuration from ~/.sentinel/config.yaml."""
    if not CONFIG_PATH.exists():
        return DEFAULT_CONFIG
        
    try:
        with open(CONFIG_PATH, "r") as f:
            user_config = yaml.safe_load(f)
            if not user_config:
                return DEFAULT_CONFIG
                
            # Merge user config with defaults to prevent missing key errors
            merged = DEFAULT_CONFIG.copy()
            for key, val in user_config.items():
                if isinstance(val, dict) and key in merged:
                    merged[key].update(val)
            return merged
    except Exception:
        return DEFAULT_CONFIG

# Export the Config dictionary to be imported across the app
Config = load_config()