#!/bin/bash
# ============================================================
# 🧠 IntelliPRRSV2 — FULL REAL DATA PIPELINE (CLEAN & SAFE)
# ============================================================

set -euo pipefail
trap 'echo "❌ Pipeline failed at line $LINENO"; mysql -u prrsvuser -pPrrsvPass2026! intelliprrsv2_db -e "UPDATE pipeline_runs SET status=\"failed\", end_time=NOW() WHERE run_id=$RUN_ID;"; exit 1' ERR

# ------------------------------------------------------------
# Environment
# ------------------------------------------------------------
BASE_DIR="/home/abubakar/intelliprrsv2"
LOG_DIR="$BASE_DIR/logs"

export PYTHONPATH="/home/abubakar"
mkdir -p "$LOG_DIR"

source "$BASE_DIR/venv/bin/activate"

echo "==============================================="
echo "🧠 IntelliPRRSV2 — FULL REAL DATA PIPELINE"
echo "📅 Started: $(date)"
echo "==============================================="

# ------------------------------------------------------------
# AUTHORITATIVE PIPELINE TIMESTAMP
# ------------------------------------------------------------
PIPELINE_TS=$(date +"%Y-%m-%d %H:%M:%S")
echo "🕒 Pipeline run timestamp: $PIPELINE_TS"

# ------------------------------------------------------------
# PIPELINE LOCK (PREVENT OVERLAP)
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
# START PIPELINE RUN
# ------------------------------------------------------------
RUN_ID=$(mysql -u prrsvuser -pPrrsvPass2026! intelliprrsv2_db -N -B -e "
INSERT INTO pipeline_runs(start_time, status, notes)
VALUES ('$PIPELINE_TS', 'running', 'ultrafast_real pipeline started');
SELECT LAST_INSERT_ID();
")

echo "🧾 Pipeline Run ID: $RUN_ID"

# ------------------------------------------------------------
# LAYER 1: Data Acquisition (FIXED)
# ------------------------------------------------------------
echo "📡 LAYER 1: Fetching PRRSV sequences..."
PIPELINE_TS="$PIPELINE_TS" PYTHONPATH=/home/abubakar python3 -m intelliprrsv2.scripts.fetch_prrsv_ultrafast \
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
PYTHONPATH=/home/abubakar python3 -m intelliprrsv2.scripts.clean_sequences_fast \
  >> "$LOG_DIR/clean.log" 2>&1
echo "✅ Cleaning complete."

# ------------------------------------------------------------
# LAYER 3: Mutation Hotspots
# ------------------------------------------------------------
echo "🔥 LAYER 3: Mutation hotspots..."
PIPELINE_TS="$PIPELINE_TS" PYTHONPATH=/home/abubakar python3 -m intelliprrsv2.scripts.mutation_hotspot_fast \
  >> "$LOG_DIR/mutation.log" 2>&1
echo "✅ Mutation hotspots saved."

# ------------------------------------------------------------
# LAYER 3.5: ORF Mapping
# ------------------------------------------------------------
echo "🧩 LAYER 3.5: ORF mapping..."
PYTHONPATH=/home/abubakar python3 -m intelliprrsv2.scripts.add_orf_annotations_auto \
  >> "$LOG_DIR/orf_mapping.log" 2>&1
echo "✅ ORF annotation complete."

# ------------------------------------------------------------
# LAYER 4: Phylogeny & Lineages
# ------------------------------------------------------------
echo "🌳 LAYER 4: Phylogeny & lineages..."
PIPELINE_TS="$PIPELINE_TS" PYTHONPATH=/home/abubakar python3 -m intelliprrsv2.scripts.build_phylogeny_optimized \
  >> "$LOG_DIR/lineage.log" 2>&1
echo "✅ Lineages & tree saved."

# ------------------------------------------------------------
# LAYER 5: Vaccine Escape
# ------------------------------------------------------------
echo "💉 LAYER 5: Vaccine escape prediction..."
PIPELINE_TS="$PIPELINE_TS" PYTHONPATH=/home/abubakar python3 -m intelliprrsv2.scripts.vaccine_escape_ai_fast \
  >> "$LOG_DIR/vaccine.log" 2>&1
echo "✅ Vaccine escape updated."

# ------------------------------------------------------------
# LAYER 6: miRNA Interactions
# ------------------------------------------------------------
echo "🧠 LAYER 6: miRNA interactions..."
PIPELINE_TS="$PIPELINE_TS" PYTHONPATH=/home/abubakar python3 -m intelliprrsv2.scripts.mirna_interaction_fast_real \
  >> "$LOG_DIR/mirna.log" 2>&1
echo "✅ miRNA interactions saved."

# ------------------------------------------------------------
# LAYER 7: Geographical Metadata
# ------------------------------------------------------------
echo "🗺️  LAYER 7: Geographical metadata..."
PIPELINE_TS="$PIPELINE_TS" PYTHONPATH=/home/abubakar python3 -m intelliprrsv2.scripts.extract_geo_metadata \
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
# COMPLETE PIPELINE RUN
# ------------------------------------------------------------
mysql -u prrsvuser -pPrrsvPass2026! intelliprrsv2_db -e "
UPDATE pipeline_runs
SET status='complete',
    end_time=NOW()
WHERE run_id=$RUN_ID;
"

echo "==============================================="
echo "🏁 PIPELINE COMPLETE — $(date)"
echo "✅ Run ID: $RUN_ID"
echo "✅ Logs: $LOG_DIR"
echo "==============================================="

deactivate
