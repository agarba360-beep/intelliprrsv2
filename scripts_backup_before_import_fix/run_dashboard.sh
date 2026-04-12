#!/bin/bash
# ===============================================
# 🧠 IntelliPRRSV2 — Dashboard Launcher
# ===============================================

BASE_DIR="/home/abubakar/intelliprrsv2"
LOG_DIR="$BASE_DIR/logs"
PORT=8502  # unique port for PRRSV dashboard

echo "==============================================="
echo "📊 Launching IntelliPRRSV2 Streamlit Dashboard"
echo "📅 Started: $(date)"
echo "==============================================="

cd "$BASE_DIR"
source venv/bin/activate

DASHBOARD_DIR="$BASE_DIR/dashboard"

if [ -d "$DASHBOARD_DIR" ]; then
    cd "$DASHBOARD_DIR"
    echo "⚙️ Starting Streamlit dashboard on port $PORT..."
    nohup streamlit run app.py --server.port $PORT > "$LOG_DIR/dashboard.log" 2>&1 &
    echo "✅ Dashboard running at: http://$(hostname -I | awk '{print $1}'):${PORT}"
else
    echo "❌ Dashboard directory not found at $DASHBOARD_DIR"
fi

# Keep the venv active if user wants to interact
echo "✅ Dashboard started. Check logs at: $LOG_DIR/dashboard.log"

