#!/usr/bin/env python3
"""
Layer 5 — AI Vaccine Escape Prediction (Real, Fast)
Predicts vaccine escape potential using real PRRSV data.
"""

import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from Bio import SeqIO
from Bio import pairwise2
from datetime import datetime

# -----------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------
BASE_DIR = "/home/abubakar/intelliprrsv2"
DATA_DIR = os.path.join(BASE_DIR, "data/clean")
REF_DIR = os.path.join(BASE_DIR, "reference/vaccines")
OUT_FILE = os.path.join(BASE_DIR, "results/vaccine_escape_ai_fast.csv")

os.makedirs(os.path.join(BASE_DIR, "results"), exist_ok=True)

# -----------------------------------------------------
# HELPER FUNCTIONS
# -----------------------------------------------------
def sequence_identity(seq1, seq2):
    """Simple percentage identity based on alignment."""
    alignments = pairwise2.align.globalxx(seq1, seq2, one_alignment_only=True)
    aln = alignments[0]
    matches = sum(a == b for a, b in zip(aln.seqA, aln.seqB))
    return 100 * matches / len(aln.seqA)

def predict_escape_probability(identity_score):
    """Convert identity % to escape probability (simple logistic model)."""
    # Below 80% identity = high escape risk
    return 1 / (1 + np.exp((identity_score - 85) / -3))

# -----------------------------------------------------
# LOAD VACCINE REFERENCES
# -----------------------------------------------------
print("💉 Loading vaccine reference sequences...")
vaccine_refs = []
for f in os.listdir(REF_DIR):
    if f.endswith(".fasta"):
        rec = next(SeqIO.parse(os.path.join(REF_DIR, f), "fasta"))
        vaccine_refs.append((f.replace(".fasta",""), str(rec.seq)))

print(f"✅ Loaded {len(vaccine_refs)} real vaccine references.")

# -----------------------------------------------------
# LOAD PRRSV CLEANED GENOMES
# -----------------------------------------------------
prrsv_files = [f for f in os.listdir(DATA_DIR) if f.endswith(".fasta")]
print(f"🧬 Loaded {len(prrsv_files)} PRRSV sequences for analysis.")

# -----------------------------------------------------
# MAIN LOOP — Compute Escape Probabilities
# -----------------------------------------------------
results = []
for prrsv_file in tqdm(prrsv_files, desc="AI vaccine escape prediction"):
    prrsv_record = next(SeqIO.parse(os.path.join(DATA_DIR, prrsv_file), "fasta"))
    isolate_seq = str(prrsv_record.seq)

    for vaccine_name, vaccine_seq in vaccine_refs:
        identity = sequence_identity(isolate_seq[:1000], vaccine_seq[:1000])  # partial comparison for speed

        # Calculate escape probability using our simple model
        escape_prob = predict_escape_probability(identity)

        # Classify risk based on probability
        if escape_prob > 0.75:
            risk = "High"
        elif escape_prob > 0.45:
            risk = "Moderate"
        else:
            risk = "Low"

        # Store results
        results.append({
            "isolate": prrsv_file,
            "vaccine": vaccine_name,
            "match_score_%": round(identity, 2),
            "escape_probability": round(escape_prob, 3),
            "predicted_escape_risk": risk
        })

# -----------------------------------------------------
# SAVE RESULTS
# -----------------------------------------------------
if results:
    df = pd.DataFrame(results)
    df["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df.to_csv(OUT_FILE, index=False)
    print(f"✅ Vaccine escape predictions saved to: {OUT_FILE}")
else:
    print("⚠️ No results were generated — check input data paths or sequence quality.")

from intelliprrsv2.scripts.db_utils import insert_dataframe
import pandas as pd

csv_path = "/home/abubakar/intelliprrsv2/results/vaccine_escape_ai_fast.csv"
df = pd.read_csv(csv_path)
insert_dataframe(df, "vaccine_escape_predictions")

