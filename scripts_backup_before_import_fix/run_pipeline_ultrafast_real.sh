#!/bin/bash
# ============================================================
# 🧠 IntelliPRRSV2 — FULL REAL DATA PIPELINE (CLEAN & SAFE)
# ============================================================

set -euo pipefail
trap 'echo "❌ Error on line $LINENO while running: $BASH_COMMAND"' ERR

# ------------------------------------------------------------
# Environment
# ------------------------------------------------------------
export PYTHONPATH=/home/abubakar
BASE_DIR="/home/abubakar/intelliprrsv2"
LOG_DIR="$BASE_DIR/logs"
mkdir -p "$LOG_DIR"

source "$BASE_DIR/venv/bin/activate"

echo "==============================================="
echo "🧠 IntelliPRRSV2 — FULL REAL DATA PIPELINE"
echo "📅 Started: $(date)"
echo "==============================================="

# ------------------------------------------------------------
# AUTHORITATIVE PIPELINE TIMESTAMP (SINGLE SOURCE OF TRUTH)
# ------------------------------------------------------------
PIPELINE_TS=$(date +"%Y-%m-%d %H:%M:%S")
echo "🕒 Pipeline run timestamp: $PIPELINE_TS"

# ------------------------------------------------------------
# 🚫 PIPELINE LOCK (PREVENT OVERLAP)
# ------------------------------------------------------------
RUNNING_COUNT=$(mysql -u prrsvuser -pPrrsvPass2026! intelliprrsv2_db -N -B -e "
SELECT COUNT(*) FROM pipeline_runs WHERE status='running';
")

if [ "$RUNNING_COUNT" -gt 0 ]; then
  echo "❌ Another pipeline run is already in progress."
  echo "❌ Aborting to prevent data corruption."
  exit 1
fi

# ------------------------------------------------------------
# PIPELINE RUN MARKER — START (ONLY ONCE)
# ------------------------------------------------------------
RUN_ID=$(mysql -u prrsvuser -pPrrsvPass2026! intelliprrsv2_db -N -B -e "
INSERT INTO pipeline_runs(start_time, status, notes)
VALUES ('$PIPELINE_TS', 'running', 'ultrafast_real pipeline started');
SELECT LAST_INSERT_ID();
")

echo "🧾 Pipeline Run ID: $RUN_ID"

# ------------------------------------------------------------
# LAYER 1: Data Acquisition
# ------------------------------------------------------------
echo "📡 LAYER 1: Fetching PRRSV sequences..."
PIPELINE_TS="$PIPELINE_TS" python3 -m intelliprrsv2.scripts.fetch_prrsv_ultrafast \
  >> "$LOG_DIR/fetch.log" 2>&1
echo "✅ Data acquisition complete."

NEW_SEQS=$(mysql -u prrsvuser -pPrrsvPass2026! intelliprrsv2_db -N -B -e "
SELECT COUNT(*) FROM sequences
WHERE run_timestamp >= '$PIPELINE_TS';
")

mysql -u prrsvuser -pPrrsvPass2026! intelliprrsv2_db -e "
UPDATE pipeline_runs
SET new_sequences=$NEW_SEQS
WHERE run_id=$RUN_ID;
"

echo "🧬 New sequences this run: $NEW_SEQS"

# ------------------------------------------------------------
# LAYER 2: Cleaning
# ------------------------------------------------------------
echo "🧹 LAYER 2: Cleaning sequences..."
python3 "$BASE_DIR/scripts/clean_sequences_fast.py" \
  >> "$LOG_DIR/clean.log" 2>&1
echo "✅ Cleaning complete."

# ------------------------------------------------------------
# LAYER 3: Mutation Hotspots
# ------------------------------------------------------------
echo "🔥 LAYER 3: Mutation hotspots..."
PIPELINE_TS="$PIPELINE_TS" python3 "$BASE_DIR/scripts/mutation_hotspot_fast.py" \
  >> "$LOG_DIR/mutation.log" 2>&1
echo "✅ Mutation hotspots saved."

# ------------------------------------------------------------
# LAYER 3.5: ORF Mapping
# ------------------------------------------------------------
echo "🧩 LAYER 3.5: ORF mapping..."
python3 "$BASE_DIR/scripts/add_orf_annotations_auto.py" \
  >> "$LOG_DIR/orf_mapping.log" 2>&1
echo "✅ ORF annotation complete."

# ------------------------------------------------------------
# LAYER 4: Phylogeny & Lineages
# ------------------------------------------------------------
echo "🌳 LAYER 4: Phylogeny & lineages..."
PIPELINE_TS="$PIPELINE_TS" python3 "$BASE_DIR/scripts/build_phylogeny_optimized.py" \
  >> "$LOG_DIR/lineage.log" 2>&1
echo "✅ Lineages & tree saved."

# ------------------------------------------------------------
# LAYER 5: Vaccine Escape
# ------------------------------------------------------------
echo "💉 LAYER 5: Vaccine escape prediction..."
PIPELINE_TS="$PIPELINE_TS" python3 "$BASE_DIR/scripts/vaccine_escape_ai_fast.py" \
  >> "$LOG_DIR/vaccine.log" 2>&1
echo "✅ Vaccine escape updated."

# ------------------------------------------------------------
# LAYER 6: miRNA Interactions
# ------------------------------------------------------------
echo "🧠 LAYER 6: miRNA interactions..."
PIPELINE_TS="$PIPELINE_TS" python3 "$BASE_DIR/scripts/mirna_interaction_fast_real.py" \
  >> "$LOG_DIR/mirna.log" 2>&1
echo "✅ miRNA interactions saved."

# ------------------------------------------------------------
# LAYER 7: Geographical Metadata (FIXED)
# ------------------------------------------------------------
echo "🗺️  LAYER 7: Geographical metadata..."
PIPELINE_TS="$PIPELINE_TS" python3 "$BASE_DIR/scripts/extract_geo_metadata.py" \
  >> "$LOG_DIR/geo.log" 2>&1
echo "✅ Geographical metadata refreshed."

# ------------------------------------------------------------
# LAYER 8: Dashboard Refresh
# ------------------------------------------------------------
echo "📊 LAYER 8: Dashboard refresh..."
touch "$BASE_DIR/dashboard/refresh.flag"
rm -rf /home/abubakar/.streamlit/cache 2>/dev/null || true
echo "✅ Dashboard will reload on next refresh."

# ------------------------------------------------------------
# PIPELINE RUN MARKER — END (AUTHORITATIVE)
# ------------------------------------------------------------
mysql -u prrsvuser -pPrrsvPass2026! intelliprrsv2_db -e "
UPDATE pipeline_runs
SET status='complete',
    end_time=NOW()
WHERE run_id=$RUN_ID;
"

# ------------------------------------------------------------
# COMPLETION
# ------------------------------------------------------------
echo "==============================================="
echo "🏁 PIPELINE COMPLETE — $(date)"
echo "✅ Run ID: $RUN_ID"
echo "✅ Logs: $LOG_DIR"
echo "==============================================="

deactivate

