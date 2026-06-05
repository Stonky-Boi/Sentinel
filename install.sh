#!/bin/bash
set -e
echo "Installing Sentinel Framework..."

# 1. Create the storage directories and config file
echo "-> Creating storage layers at ~/.sentinel..."
mkdir -p ~/.sentinel/logs
mkdir -p ~/.sentinel/reports

CONFIG_FILE=~/.sentinel/config.yaml
if [ ! -f "$CONFIG_FILE" ]; then
    echo "-> Generating default config.yaml..."
    cat << EOF > "$CONFIG_FILE"
# Sentinel Master Configuration
kafka:
  bootstrap_servers: "localhost:9092"
  topic_raw: "logs_raw"
  topic_dlq: "logs_dead_letter"
  consumer_group: "sentinel_triage_group"
  max_workers: 4

qdrant:
  url: "http://localhost:6333"
  collection: "network_logs"
  vector_size: 768

llm:
  embedding_model: "nomic-embed-text"
  reasoning_model: "qwen2.5-coder"
  triage_model: "qwen2.5-coder"

rag:
  retrieval_limit: 2
EOF
fi

# 2. Setup the Python Virtual Environment
echo "-> Setting up isolated Python environment..."
python3 -m venv venv
source venv/bin/activate

# 3. Install dependencies
echo "-> Installing dependencies..."
pip install -q --upgrade pip
pip install -qr requirements.txt

# 4. Create the Global Executable Wrapper
echo "-> Generating global command-line wrapper..."

# Get the absolute path of the current directory
PROJECT_DIR="$PWD"

# Create a temporary bash script
cat << EOF > sentinel_temp_wrapper
#!/bin/bash
# This wrapper activates the Sentinel virtual environment and passes arguments to main.py
source "$PROJECT_DIR/venv/bin/activate"
python "$PROJECT_DIR/main.py" "\$@"
EOF

# Make the wrapper executable
chmod +x sentinel_temp_wrapper

# Move it to a global binary path (requires sudo)
echo "-> Installing 'sentinel' command to /usr/local/bin (may prompt for password)..."
sudo mv sentinel_temp_wrapper /usr/local/bin/sentinel

echo ""
echo "Installation Complete!"
echo "You can now run Sentinel from ANY directory using the 'sentinel' command."
echo ""
echo "Try it out:"
echo "  sentinel status"
echo "  sentinel start"
echo "  sentinel monitor"