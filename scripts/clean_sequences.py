import os
import json
from Bio import SeqIO
from datetime import datetime

# === Configuration ===
RAW_DIR = "/home/abubakar/intelliprrsv2/data/raw/"
CLEAN_DIR = "/home/abubakar/intelliprrsv2/data/clean/"
REPORT_FILE = "/home/abubakar/intelliprrsv2/results/quality_report.json"

MIN_LENGTH = 600           # minimum for partial ORF
MIN_GENOME_LENGTH = 10000  # minimum for near-full genome

report = {
    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "total_files": 0,
    "cleaned_files": 0,
    "removed_short": [],
    "fixed_headers": [],
    "orf_predictions": {}
}

# --------------------------------------------
# Function: Normalize FASTA Header
# --------------------------------------------
def normalize_header(record):
    """Ensure FASTA header has no spaces or pipe symbols."""
    old_header = record.id
    new_header = old_header.replace("|", "_").replace(" ", "_")
    if old_header != new_header:
        report["fixed_headers"].append(old_header)
    record.id = new_header
    record.description = ""
    return record

# --------------------------------------------
# Function: Predict ORF5–ORF7 Start Motifs
# --------------------------------------------
def predict_orfs(seq):
    """Predict approximate ORF start positions using known motifs."""
    sequence = str(seq).upper()
    motifs = {
        "ORF5": "ATGGGG",
        "ORF6": "ATGTTG",
        "ORF7": "ATGAGT"
    }
    results = {}
    for orf, motif in motifs.items():
        pos = sequence.find(motif)
        results[orf] = pos if pos != -1 else None
    return results

# --------------------------------------------
# Main Cleaning Pipeline
# --------------------------------------------
def clean_all():
    files = [f for f in os.listdir(RAW_DIR) if f.endswith(".fasta")]
    report["total_files"] = len(files)

    for fname in files:
        try:
            path = os.path.join(RAW_DIR, fname)
            record = SeqIO.read(path, "fasta")
            seq_len = len(record.seq)

            # Filter out too-short sequences
            if seq_len < MIN_LENGTH:
                report["removed_short"].append(fname)
                continue

            # Normalize header
            record = normalize_header(record)

            # Predict ORFs
            orfs = predict_orfs(record.seq)
            report["orf_predictions"][fname] = orfs

            # Save cleaned FASTA
            save_path = os.path.join(CLEAN_DIR, fname)
            SeqIO.write(record, save_path, "fasta")

            report["cleaned_files"] += 1

        except Exception as e:
            print(f"Error cleaning {fname}: {e}")
            continue

    # Write summary report
    with open(REPORT_FILE, "w") as r:
        json.dump(report, r, indent=2)

    print(f"✅ Cleaning completed — {report['cleaned_files']} files cleaned.")
    print(f"🧾 Report saved to {REPORT_FILE}")

if __name__ == "__main__":
    clean_all()

