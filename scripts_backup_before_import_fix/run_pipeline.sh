#!/bin/bash
source venv/bin/activate

echo "🧲 Step 1: Fetching data..."
python3 scripts/fetch_to_mysql_optimized.py

echo "🧹 Step 2: Cleaning sequences..."
python3 scripts/clean_sequences_parallel.py

echo "🔥 Step 3: Detecting mutation hotspots..."
python3 scripts/mutation_hotspot.py

echo "🌳 Step 4: Building phylogenetic tree..."
python3 scripts/build_phylogeny_optimized.py

echo "💉 Step 5: Vaccine escape prediction..."
python3 scripts/vaccine_escape_predictor_fast.py

echo "🧠 Step 6: miRNA–virus interaction analysis..."
python3 scripts/mirna_interaction_fast.py

echo "✅ All steps complete! Launching dashboard..."
cd dashboard
streamlit run app.py

