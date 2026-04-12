import os
from Bio import SeqIO
import pandas as pd
from tqdm import tqdm

# ----------------------------------------------------------
# PATHS
# ----------------------------------------------------------
CLEAN_DIR = "/home/abubakar/intelliprrsv2/data/clean/"
MIRNA_FILE = "/home/abubakar/intelliprrsv2/reference/mirna/sus_mirna.fa"
OUTPUT_FILE = "/home/abubakar/intelliprrsv2/results/mirna/mirna_interactions.csv"

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

# ----------------------------------------------------------
# LOAD MIRNA SEQUENCES
# ----------------------------------------------------------
print("📥 Loading porcine miRNAs from file...")
mirnas = {rec.id: str(rec.seq).upper() for rec in SeqIO.parse(MIRNA_FILE, "fasta")}
print(f"✅ Loaded {len(mirnas)} Sus scrofa miRNAs.")

# ----------------------------------------------------------
# DEFINE FUNCTION TO COMPUTE COMPLEMENTARITY
# ----------------------------------------------------------
def complementarity_score(mirna, target):
    """Calculate complementarity score with wobble tolerance (G-U pairs allowed)."""
    pairs = {
        "A": ["U", "T"],
        "U": ["A", "G"],
        "T": ["A", "G"],
        "G": ["C", "U", "T"],
        "C": ["G"]
    }

    score = 0
    for m, t in zip(mirna, target):
        if t in pairs.get(m, []):
            score += 1
    return score / len(mirna)

# ----------------------------------------------------------
# SCAN FOR MIRNA BINDING SITES
# ----------------------------------------------------------
results = []
print("🔬 Scanning PRRSV sequences for miRNA binding sites...")

for f in tqdm(os.listdir(CLEAN_DIR)):
    if not f.endswith(".fasta"):
        continue

    seq_path = os.path.join(CLEAN_DIR, f)
    seq_record = next(SeqIO.parse(seq_path, "fasta"))
    seq = str(seq_record.seq).upper()

    for mirna_id, mirna_seq in mirnas.items():
        if len(mirna_seq) < 18 or len(mirna_seq) > 26:
            continue

        best_score = 0
        for i in range(0, len(seq) - len(mirna_seq)):
            region = seq[i : i + len(mirna_seq)]
            score = complementarity_score(mirna_seq, region)
            if score > best_score:
                best_score = score

        # Adjusted threshold (0.55 works well in literature)
        if best_score > 0.55:
            results.append({
                "accession": f.replace(".fasta", ""),
                "miRNA": mirna_id,
                "binding_score": round(best_score, 3)
            })

# ----------------------------------------------------------
# SAVE RESULTS
# ----------------------------------------------------------
df = pd.DataFrame(results)
if len(df) > 0:
    df.sort_values(by="binding_score", ascending=False, inplace=True)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved {len(df)} strong interactions → {OUTPUT_FILE}")
else:
    print("⚠️ No strong miRNA–virus interactions detected above threshold.")

