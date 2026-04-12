#!/bin/bash
# =======================================================
# INTELLIPRRSV2 AUTOMATION SCRIPT
# Fetch + Metadata Extraction + Logging
# Author: Abubakar / AI Concepts Ltd
# =======================================================

# === CONFIGURATION ===
PROJECT_DIR="/home/abubakar/intelliprrsv2"
VENV_PATH="$PROJECT_DIR/venv"
LOGFILE="$PROJECT_DIR/logs/fetch.log"
PY_FETCH="$PROJECT_DIR/scripts/fetch_ncbi.py"
PY_EXTRACT="$PROJECT_DIR/scripts/extract_metadata.py"

# === START LOGGING ===
echo "=============================================" >> $LOGFILE
echo "🚀 Starting INTELLIPRRSV2 pipeline: $(date)" >> $LOGFILE
echo "=============================================" >> $LOGFILE

# === ACTIVATE ENVIRONMENT ===
cd $PROJECT_DIR
source $VENV_PATH/bin/activate

# === STEP 1: RUN FETCHER ===
echo "🔍 Fetching new PRRSV data..." >> $LOGFILE
python3 $PY_FETCH >> $LOGFILE 2>&1
FETCH_STATUS=$?

# === STEP 2: RUN METADATA EXTRACTOR ===
if [ $FETCH_STATUS -eq 0 ]; then
    echo "🧹 Extracting and cleaning metadata..." >> $LOGFILE
    python3 $PY_EXTRACT >> $LOGFILE 2>&1
else
    echo "⚠️ Warning: Fetch script returned an error. Continuing to metadata extraction anyway..." >> $LOGFILE
    python3 $PY_EXTRACT >> $LOGFILE 2>&1
fi

# === STEP 3: CLEANUP ===
deactivate

echo "✅ Pipeline completed successfully: $(date)" >> $LOGFILE
echo "---------------------------------------------" >> $LOGFILE

