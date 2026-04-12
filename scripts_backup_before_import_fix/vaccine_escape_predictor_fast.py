import os
import pandas as pd
from Bio import SeqIO
from Bio.Align import PairwiseAligner
from joblib import Parallel, delayed
from tqdm import tqdm
from datetime import datetime

# ----------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------
CLEAN_DIR = "/home/abubakar/intelliprrsv2/data/clean/"
VACCINE_DIR = "/home/abubakar/intelliprrsv2/reference/vaccines/"
LINEAGE_FILE = "/home/abubakar/intelliprrsv2/results/phylo/real_lineage_table.csv"
RESULT_FILE = "/home/abubakar/intelliprrsv2/results/vaccine/vaccine_escape_results.csv"
os.makedirs(os.path.dirname(RESULT_FILE), exist_ok=True)

ESCAPE_THRESHOLD = 85.0  # percent similarity

# ----------------------------------------------------------
# LOAD DATA
# ----------------------------------------------------------
print("📥 Loading isolate and vaccine data...")

# Load lineage info
lineage_df = pd.read_csv(LINEAGE_FILE)
print(f"✅ Loaded {len(lineage_df)} lineage records.")

# Load vaccine reference genomes
vaccine_refs = {}
for f in os.listdir(VACCINE_DIR):
    if f.endswith(".fasta"):
        name = f.replace(".fasta", "")
        seq = str(next(SeqIO.parse(os.path.join(VACCINE_DIR, f), "fasta")).seq)
        vaccine_refs[name] = seq
print(f"✅ Loaded {len(vaccine_refs)} vaccine reference genomes.")

# ----------------------------------------------------------
# FUNCTION: Compute similarity
# ----------------------------------------------------------
def compute_similarity(isolate_file):
    iso_path = os.path.join(CLEAN_DIR, isolate_file)
    isolate_id = isolate_file.replace(".fasta", "")
    seq = str(next(SeqIO.parse(iso_path, "fasta")).seq)
    aligner = PairwiseAligner()
    aligner.mode = "global"

    best_vaccine, best_score = None, 0
    for vname, vseq in vaccine_refs.items():
        score = aligner.score(seq, vseq)
        percent_sim = (score / max(len(seq), len(vseq))) * 100
        if percent_sim > best_score:
            best_score = percent_sim
            best_vaccine = vname

    return {
        "accession": isolate_id,
        "best_vaccine_match": best_vaccine,
        "similarity_%": round(best_score, 2),
        "escape_flag": "⚠️ Potential Escape" if best_score < ESCAPE_THRESHOLD else "✅ Within Vaccine Range",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

# ----------------------------------------------------------
# RUN IN PARALLEL
# ----------------------------------------------------------
files = [f for f in os.listdir(CLEAN_DIR) if f.endswith(".fasta")]
print(f"🧬 Computing isolate–vaccine similarity for {len(files)} sequences...")

results = Parallel(n_jobs=8)(
    delayed(compute_similarity)(f) for f in tqdm(files)
)

df = pd.DataFrame(results)
df = df.merge(lineage_df, on="accession", how="left")

# Save results
df.to_csv(RESULT_FILE, index=False)
print(f"✅ Saved vaccine escape results → {RESULT_FILE}")

# Summary
escape_count = len(df[df["escape_flag"].str.contains("Escape")])
print(f"⚠️ {escape_count} potential escape isolates detected.")
print(df.head(10))

