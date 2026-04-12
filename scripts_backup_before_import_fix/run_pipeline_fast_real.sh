#!/bin/bash
# ======================================================
# 🧠 IntelliPRRSV2 — FAST Full Real Data Pipeline (v2026)
# Author: Abubakar
# ======================================================

set -e  # stop if any command fails

DATE=$(date +"%Y%m%d_%H%M%S")
LOG_DIR="logs"
mkdir -p $LOG_DIR
LOG_FILE="$LOG_DIR/pipeline_real_${DATE}.log"

echo "===============================================" | tee -a $LOG_FILE
echo "🧠 IntelliPRRSV2 — FAST Full Real Data Mode" | tee -a $LOG_FILE
echo "📅 Started: $(date)" | tee -a $LOG_FILE
echo "===============================================" | tee -a $LOG_FILE

# Activate environment
source ~/intelliprrsv2/venv/bin/activate

run_step () {
    echo "📡 Running: $1" | tee -a $LOG_FILE
    python3 $1 >> $LOG_FILE 2>&1
    echo "✅ Completed: $1" | tee -a $LOG_FILE
}

# ------------------------------------------------------
# Step 1: Fetch — only if needed (real NCBI data)
# ------------------------------------------------------
RAW_COUNT=$(ls -1 data/raw | wc -l)
if [ "$RAW_COUNT" -lt 400 ]; then
    echo "🔍 Fetching real PRRSV genomes from NCBI..." | tee -a $LOG_FILE
    run_step "scripts/fetch_to_mysql_parallel.py"
else
    echo "⚡ Using existing $RAW_COUNT real PRRSV sequences from NCBI." | tee -a $LOG_FILE
fi

# ------------------------------------------------------
# Step 2: Cleaning — remove short/ambiguous
# ------------------------------------------------------
run_step "scripts/clean_sequences_parallel.py"

# ------------------------------------------------------
# Step 3: Mutation Hotspots — lightweight MAFFT (real alignment)
# ------------------------------------------------------
echo "🧬 Running real MAFFT alignment (optimized)..." | tee -a $LOG_FILE
sed -i 's/--auto/--retree 1 --maxiterate 0/g' scripts/mutation_hotspot.py || true
run_step "scripts/mutation_hotspot.py"

# ------------------------------------------------------
# Step 4: Phylogenetic Tree — FastTree on real alignment
# ------------------------------------------------------
run_step "scripts/build_phylogeny_optimized.py"

# ------------------------------------------------------
# Step 5: Vaccine Escape — real isolate vs vaccine comparison
# ------------------------------------------------------
run_step "scripts/vaccine_escape_predictor_fast.py"

# ------------------------------------------------------
# Step 6: miRNA Interaction — real Sus scrofa miRNAs
# ------------------------------------------------------
run_step "scripts/mirna_interaction_fast.py"

# ------------------------------------------------------
# Step 7: Dashboard (real data)
# ------------------------------------------------------
echo "📊 Launching Streamlit dashboard (background)..." | tee -a $LOG_FILE
cd dashboard
nohup streamlit run app.py > ../logs/dashboard_${DATE}.log 2>&1 &
cd ..

echo "✅ FAST Full Real Pipeline Complete at $(date)" | tee -a $LOG_FILE
echo "📁 Results saved under ~/intelliprrsv2/results/" | tee -a $LOG_FILE
echo "🧠 Dashboard log: logs/dashboard_${DATE}.log"

