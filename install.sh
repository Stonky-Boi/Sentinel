#!/bin/bash
set -e
echo "Installing Sentinel Framework..."

# 1. Create the hidden storage directories
echo "-> Creating hidden storage layers at ~/.sentinel..."
mkdir -p ~/.sentinel/logs
mkdir -p ~/.sentinel/reports

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