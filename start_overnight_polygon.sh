#!/bin/bash
# Start Polygon Stock Data Gathering - Overnight Run
# Run with: ./start_overnight_polygon.sh

cd /Users/dougstrouth/github_noicloud/edgar

# Activate virtual environment
source .venv/bin/activate

# Create log directory if it doesn't exist
mkdir -p logs

# Create timestamp for log file
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOGFILE="logs/overnight_polygon_${TIMESTAMP}.log"

echo "=========================================="
echo "Starting Polygon Stock Data Gatherer"
echo "=========================================="
echo "Log file: $LOGFILE"
echo "PID file: /tmp/polygon_overnight.pid"
echo ""
echo "Features:"
echo "  ⏰ Max runtime: 9 hours"
echo "  📊 Rate limit: 5 calls/min"
echo "  💾 DB write: every 15 min"
echo "  🔄 Auto-resume: skips existing data"
echo ""

# Run in background
nohup .venv/bin/python main.py gather-stocks-polygon \
  --mode append \
  --lookback-years 2 \
  > "$LOGFILE" 2>&1 &

# Save PID
PID=$!
echo $PID > /tmp/polygon_overnight.pid

echo "Process started with PID: $PID"
echo ""
echo "Monitor with:"
echo "  tail -f $LOGFILE"
echo ""
echo "Stop with:"
echo "  kill \$(cat /tmp/polygon_overnight.pid)"
echo ""
echo "Check status:"
echo "  ps -p \$(cat /tmp/polygon_overnight.pid)"
echo ""
