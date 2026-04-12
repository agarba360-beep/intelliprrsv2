#!/usr/bin/env python3
# ============================================================
# 🧠 IntelliPRRSV2 — Real miRNA–PRRSV Binding (Fast Mode)
# ============================================================

import os
import pandas as pd
from Bio import SeqIO
from tqdm import tqdm
from datetime import datetime
from joblib import Parallel, delayed

from db_utils import insert_dataframe  # ✅ FIXED IMPORT

# ============================================================
# 📂 PATH CONFIGURATION
# ============================================================

CLEAN_DIR = "/home/abubakar/intelliprrsv2/data/clean/"
MIRNA_FILE = "/home/abubakar/intelliprrsv2/reference/mirna/sus_mirna.fa"
OUT_FILE = "/home/abubakar/intelliprrsv2/results/mirna_interactions_real.csv"

os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)

# ============================================================
# 🕒 AUTHORITATIVE RUN TIMESTAMP
# ============================================================

RUN_TIMESTAMP = datetime.utcnow()

# ============================================================
# 🧬 LOAD miRNA REFERENCES
# ============================================================

mirnas = {
    m.id: str(m.seq).upper().replace("T", "U")
    for m in SeqIO.parse(MIRNA_FILE, "fasta")
    if set(str(m.seq).upper()) <= {"A", "U", "G", "C"}
}

# ============================================================
# 🔬 SCORING FUNCTIONS
# ============================================================

def comp_score(m, v):
    pair = {"A": "U", "U": "A", "G": "C", "C": "G"}
    score = sum(
        1 if pair.get(a) == b else 0.5 if a == b else 0
        for a, b in zip(m, v)
    )
    return score / len(m)


def scan(file_path):
    record = next(SeqIO.parse(file_path, "fasta"))
    seq = str(record.seq).upper().replace("T", "U")
    accession = record.id

    results = []

    for miRNA, mi_seq in mirnas.items():
        best_score = 0

        for i in range(0, len(seq) - len(mi_seq), 30):
            score = comp_score(mi_seq, seq[i:i + len(mi_seq)])
            if score > best_score:
                best_score = score

        if best_score >= 0.5:
            results.append({
                "miRNA": miRNA,
                "target_genome": accession,
                "binding_score": best_score,
                "energy_kcal": round(-10 * best_score, 2),
            })

    return results

# ============================================================
# 🚀 EXECUTION
# ============================================================

files = [
    os.path.join(CLEAN_DIR, f)
    for f in os.listdir(CLEAN_DIR)
    if f.endswith(".fasta")
]

results = [
    r
    for sub in Parallel(n_jobs=6)(
        delayed(scan)(f) for f in tqdm(files, desc="Scanning genomes")
    )
    for r in sub
]

df = pd.DataFrame(results)

# ============================================================
# ✅ APPLY RUN TIMESTAMP
# ============================================================

df["run_timestamp"] = RUN_TIMESTAMP

# ============================================================
# 💾 SAVE + INSERT
# ============================================================

df.to_csv(OUT_FILE, index=False)
print(f"✅ Saved {len(df):,} miRNA–PRRSV interactions → {OUT_FILE}")

insert_dataframe(df, "mirna_interactions")

print("✅ miRNA interactions successfully written to MySQL.")

