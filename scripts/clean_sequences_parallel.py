import os
import json
from Bio import SeqIO
from Bio.Seq import Seq
from datetime import datetime
from joblib import Parallel, delayed
from tqdm import tqdm

# --------------------------------------------------------
# DIRECTORIES
# --------------------------------------------------------
RAW_DIR = "/home/abubakar/intelliprrsv2/data/raw/"
CLEAN_DIR = "/home/abubakar/intelliprrsv2/data/clean/"
REPORT_FILE = "/home/abubakar/intelliprrsv2/results/quality_report.json"
os.makedirs(CLEAN_DIR, exist_ok=True)
os.makedirs(os.path.dirname(REPORT_FILE), exist_ok=True)

# --------------------------------------------------------
# FILTER SETTINGS
# --------------------------------------------------------
MIN_LENGTH = 600
MIN_GENOME_LENGTH = 10000

# --------------------------------------------------------
# GLOBAL REPORT (shared dictionary)
# --------------------------------------------------------
report = {
    "total_files": 0,
    "cleaned_files": 0,
    "removed_short": [],
    "fixed_headers": [],
    "orf_predictions": {}
}

# --------------------------------------------------------
# FUNCTIONS
# --------------------------------------------------------
def normalize_header(record):
    """Standardize FASTA headers (remove illegal chars)."""
    old_header = record.id
    new_header = record.id.replace("|", "_").replace(" ", "_").replace(":", "_")
    if old_header != new_header:
        report["fixed_headers"].append(old_header)
    record.id = new_header
    record.description = ""
    return record


def predict_orfs(seq):
    """Predict ORF5, ORF6, ORF7 approximate start positions."""
    sequence = str(seq).upper()
    motifs = {
        "ORF5": "ATGGGG",
        "ORF6": "ATGTTG",
        "ORF7": "ATGAGT"
    }
    results = {}
    for name, motif in motifs.items():
        pos = sequence.find(motif)
        results[name] = pos if pos != -1 else None
    return results


def process_one(fasta_file):
    """Process a single FASTA file."""
    try:
        path = os.path.join(RAW_DIR, fasta_file)
        out_path = os.path.join(CLEAN_DIR, fasta_file)

        # Skip if already cleaned
        if os.path.exists(out_path):
            return ("cached", fasta_file)

        record = SeqIO.read(path, "fasta")
        seq_len = len(record.seq)

        # Filter too short
        if seq_len < MIN_LENGTH:
            return ("short", fasta_file)

        record = normalize_header(record)
        orfs = predict_orfs(record.seq)

        SeqIO.write(record, out_path, "fasta")

        return ("cleaned", (fasta_file, orfs))

    except Exception as e:
        return ("error", f"{fasta_file}: {e}")


# --------------------------------------------------------
# MAIN PIPELINE
# --------------------------------------------------------
if __name__ == "__main__":
    files = [f for f in os.listdir(RAW_DIR) if f.endswith(".fasta")]
    report["total_files"] = len(files)
    print(f"🧹 Cleaning {len(files)} raw FASTA files...")

    results = Parallel(n_jobs=8)(
        delayed(process_one)(f) for f in tqdm(files, desc="Cleaning FASTA files")
    )

    for status, data in results:
        if status == "short":
            report["removed_short"].append(data)
        elif status == "cleaned":
            fname, orfs = data
            report["cleaned_files"] += 1
            report["orf_predictions"][fname] = orfs
        elif status == "cached":
            pass
        elif status == "error":
            print(f"⚠️ Error: {data}")

    with open(REPORT_FILE, "w") as r:
        json.dump(report, r, indent=2)

    print("✅ Cleaning complete. Files saved to /data/clean/")
    print(f"Total: {report['total_files']}, Cleaned: {report['cleaned_files']}, Removed: {len(report['removed_short'])}")

