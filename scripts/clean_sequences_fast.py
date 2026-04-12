import os
from Bio import SeqIO
from joblib import Parallel, delayed
from tqdm import tqdm

RAW_DIR = "/home/abubakar/intelliprrsv2/data/raw/"
CLEAN_DIR = "/home/abubakar/intelliprrsv2/data/clean/"
os.makedirs(CLEAN_DIR, exist_ok=True)

# ----------------------------------------
# Cleaning rules
# ----------------------------------------
def clean_file(file):
    if not file.endswith(".fasta"):
        return None

    file_path = os.path.join(RAW_DIR, file)
    try:
        seq_record = next(SeqIO.parse(file_path, "fasta"))
        seq = str(seq_record.seq).upper()

        # Rule 1: Minimum length
        if len(seq) < 1000:
            return f"❌ Too short: {file}"

        # Rule 2: Remove ambiguous bases
        seq = seq.replace("N", "")

        # Rule 3: Save cleaned FASTA
        clean_path = os.path.join(CLEAN_DIR, file)
        SeqIO.write(seq_record, clean_path, "fasta")
        return f"✅ Cleaned: {file}"
    except Exception as e:
        return f"⚠️ {file} failed: {e}"

# ----------------------------------------
# Run parallel cleaning
# ----------------------------------------
files = [f for f in os.listdir(RAW_DIR) if f.endswith(".fasta")]
print(f"🧹 Cleaning {len(files)} sequences...")
results = Parallel(n_jobs=8)(
    delayed(clean_file)(f) for f in tqdm(files, desc="Processing")
)

for r in results:
    if r: print(r)

print("✅ Cleaning complete — results saved in /data/clean/")

