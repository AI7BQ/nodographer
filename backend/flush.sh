#!/bin/bash
# Flush the mesh map database
# This script safely stops the polling daemon, clears all data from the node_info table, and restarts it

SCRIPT_DIR="/srv/meshmap/backend"
POLLING_SCRIPT="$SCRIPT_DIR/meshmapPoller.py"
VENV_PATH="$SCRIPT_DIR/venv"

if [ ! -f "$POLLING_SCRIPT" ]; then
    echo "Error: Could not find pollingScript.py at $POLLING_SCRIPT"
    exit 1
fi

# Activate the virtual environment
if [ ! -d "$VENV_PATH" ]; then
    echo "Error: Virtual environment not found at $VENV_PATH"
    exit 1
fi

source "$VENV_PATH/bin/activate"

# Change to the script directory
cd "$SCRIPT_DIR"

echo "Stopping meshmapPoller service..."
systemctl stop meshmapPoller.service
if [ $? -ne 0 ]; then
    echo "Warning: Could not stop pollingScript service (it may not be running)"
fi

sleep 2

echo "Flushing mesh map database..."
"$VENV_PATH/bin/python3" "$POLLING_SCRIPT" --flush

sleep 2

echo "Starting meshmapPoller service..."
systemctl start meshmapPoller.service