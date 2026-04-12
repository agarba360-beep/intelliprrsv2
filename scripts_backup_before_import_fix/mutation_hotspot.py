import os
import pandas as pd
from Bio import SeqIO, AlignIO
import subprocess
from collections import Counter
from datetime import datetime

# --------------------------------------------------------
# Configuration
# --------------------------------------------------------
CLEAN_DIR = "/home/abubakar/intelliprrsv2/data/clean/"
ALIGN_DIR = "/home/abubakar/intelliprrsv2/temp/alignment/"
ALIGN_FILE = os.path.join(ALIGN_DIR, "prrsv_alignment.fasta")
HOTSPOT_FILE = "/home/abubakar/intelliprrsv2/results/hotspots/mutation_hotspots.csv"

MIN_SEQS = 5            # Minimum number of sequences required
HOTSPOT_THRESHOLD = 0.05  # Minimum mutation frequency (5%)

# --------------------------------------------------------
# Load cleaned FASTA sequences
# --------------------------------------------------------
def load_sequences():
    files = [os.path.join(CLEAN_DIR, f) for f in os.listdir(CLEAN_DIR) if f.endswith(".fasta")][:50]
    if len(files) < MIN_SEQS:
        raise ValueError(f"Not enough sequences ({len(files)}). Need at least {MIN_SEQS}.")
    return files
# --------------------------------------------------------
# Run MAFFT alignment
# --------------------------------------------------------
def run_alignment(input_files):
    concat_file = os.path.join(ALIGN_DIR, "input_concat.fasta")
    os.makedirs(ALIGN_DIR, exist_ok=True)

    # Concatenate all FASTA files
    with open(concat_file, "w") as out:
        for f in input_files:
            for rec in SeqIO.parse(f, "fasta"):
                SeqIO.write(rec, out, "fasta")

    # Run MAFFT directly
    print("🚀 Running MAFFT alignment...")
    with open(ALIGN_FILE, "w") as aln_out:
        subprocess.run(
            ["mafft", "--retree 1 --maxiterate 0", concat_file],
            stdout=aln_out,
            stderr=subprocess.DEVNULL,
            check=True
        )

    print("✅ Alignment completed and saved.")
    return ALIGN_FILE

# --------------------------------------------------------
# Detect mutation frequencies
# --------------------------------------------------------
def detect_mutations(aln_path):
    alignment = AlignIO.read(aln_path, "fasta")
    aln_len = alignment.get_alignment_length()
    total = len(alignment)

    mutation_data = []

    for pos in range(aln_len):
        column = alignment[:, pos]
        counts = Counter(column)
        if "-" in counts:
            del counts["-"]
        if len(counts) > 1:  # position is variable
            major_base = counts.most_common(1)[0][0]
            for base, count in counts.items():
                if base != major_base:
                    freq = count / total
                    if freq >= HOTSPOT_THRESHOLD:
                        mutation_data.append({
                            "position": pos + 1,
                            "ref_base": major_base,
                            "alt_base": base,
                            "frequency": round(freq, 3)
                        })

    df = pd.DataFrame(mutation_data)
    df.sort_values(by="frequency", ascending=False, inplace=True)
    df["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df

# --------------------------------------------------------
# Save results
# --------------------------------------------------------
def save_hotspot_report(df):
    os.makedirs(os.path.dirname(HOTSPOT_FILE), exist_ok=True)
    df.to_csv(HOTSPOT_FILE, index=False)
    print(f"🔥 Hotspot analysis complete — {len(df)} variable positions found.")
    print(f"📁 Report saved to {HOTSPOT_FILE}")

# --------------------------------------------------------
# Main driver
# --------------------------------------------------------
def main():
    seqs = load_sequences()
    aln = run_alignment(seqs)
    df = detect_mutations(aln)
    save_hotspot_report(df)

if __name__ == "__main__":
    main()

