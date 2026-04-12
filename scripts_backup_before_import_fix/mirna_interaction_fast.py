import os
import pandas as pd
from Bio import SeqIO
from joblib import Parallel, delayed
from tqdm import tqdm
from datetime import datetime

# ----------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------
CLEAN_DIR = "/home/abubakar/intelliprrsv2/data/clean/"
MIRNA_FILE = "/home/abubakar/intelliprrsv2/reference/mirna/sus_mirna.fa"
RESULT_FILE = "/home/abubakar/intelliprrsv2/results/mirna/mirna_interactions.csv"
os.makedirs(os.path.dirname(RESULT_FILE), exist_ok=True)

MIN_BIND_SCORE = 0.75   # Strong binding threshold (0–1)
THREADS = 8             # Parallel processes

# ----------------------------------------------------------
# LOAD miRNA SEQUENCES
# ----------------------------------------------------------
print("📥 Loading porcine miRNAs...")
mirnas = {
    rec.id: str(rec.seq).upper().replace("U", "T")
    for rec in SeqIO.parse(MIRNA_FILE, "fasta")
}
print(f"✅ Loaded {len(mirnas)} Sus scrofa miRNAs from {MIRNA_FILE}")

# ----------------------------------------------------------
# COMPLEMENTARITY FUNCTION
# ----------------------------------------------------------
pairs = {"A": "T", "T": "A", "G": "C", "C": "G"}

def complementarity_score(mirna, region):
    """Fraction of matching complementary bases (A–T, G–C)."""
    matches = sum(pairs.get(m, "") == t for m, t in zip(mirna, region))
    return matches / len(mirna)

# ----------------------------------------------------------
# FUNCTION: Scan single genome
# ----------------------------------------------------------
def scan_genome(file):
    seq_path = os.path.join(CLEAN_DIR, file)
    seq_record = next(SeqIO.parse(seq_path, "fasta"))
    seq = str(seq_record.seq).upper().replace("U", "T")
    accession = file.replace(".fasta", "")

    results = []
    for mirna_id, mirna_seq in mirnas.items():
        best_score = 0
        mirna_len = len(mirna_seq)
        for i in range(0, len(seq) - mirna_len, 50):  # step = 50 bp for speed
            region = seq[i : i + mirna_len]
            score = complementarity_score(mirna_seq, region)
            if score > best_score:
                best_score = score
        if best_score >= MIN_BIND_SCORE:
            results.append({
                "accession": accession,
                "miRNA": mirna_id,
                "binding_score": round(best_score, 3),
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
    return results

# ----------------------------------------------------------
# RUN PARALLEL ANALYSIS
# ----------------------------------------------------------
files = [f for f in os.listdir(CLEAN_DIR) if f.endswith(".fasta")]
print(f"🔬 Scanning {len(files)} genomes for miRNA binding sites using {THREADS} threads...")

all_results = Parallel(n_jobs=THREADS)(
    delayed(scan_genome)(f) for f in tqdm(files)
)

# Flatten and save
flat_results = [r for sublist in all_results for r in sublist]
df = pd.DataFrame(flat_results)

if not df.empty:
    df.to_csv(RESULT_FILE, index=False)
    print(f"✅ Saved {len(df)} strong miRNA–virus interactions → {RESULT_FILE}")
else:
    print("⚠️ No strong miRNA–virus interactions found.")

