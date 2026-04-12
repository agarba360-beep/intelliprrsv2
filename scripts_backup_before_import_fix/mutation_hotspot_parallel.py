import os
import subprocess
import pandas as pd
from Bio import AlignIO
from datetime import datetime
from joblib import Parallel, delayed
from tqdm import tqdm

# ----------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------
CLEAN_DIR = "/home/abubakar/intelliprrsv2/data/clean/"
ALIGN_DIR = "/home/abubakar/intelliprrsv2/temp/alignment_chunks/"
RESULT_DIR = "/home/abubakar/intelliprrsv2/results/hotspots/"
FINAL_ALIGNMENT = "/home/abubakar/intelliprrsv2/temp/alignment/prrsv_alignment.fasta"

os.makedirs(ALIGN_DIR, exist_ok=True)
os.makedirs(RESULT_DIR, exist_ok=True)
os.makedirs(os.path.dirname(FINAL_ALIGNMENT), exist_ok=True)

CHUNK_SIZE = 500  # Number of sequences per alignment batch
THREADS = 8       # Parallel jobs
MIN_MUT_FREQ = 0.05  # 5% mutation frequency threshold

# ----------------------------------------------------------
# FUNCTION: Run MAFFT on a chunk
# ----------------------------------------------------------
def run_mafft(chunk_path, output_path):
    if os.path.exists(output_path):
        return  # Skip if already aligned
    cmd = ["mafft", "--auto", "--thread", "1", chunk_path]
    with open(output_path, "w") as out:
        subprocess.run(cmd, stdout=out, stderr=subprocess.DEVNULL)
    return output_path

# ----------------------------------------------------------
# FUNCTION: Compute mutation frequencies from aligned FASTA
# ----------------------------------------------------------
def compute_hotspots(alignment_file):
    alignment = AlignIO.read(alignment_file, "fasta")
    seqs = [str(rec.seq).upper() for rec in alignment]
    length = len(seqs[0])
    positions = []
    for i in range(length):
        bases = [s[i] for s in seqs if s[i] not in ["-", "N"]]
        if not bases:
            continue
        ref = bases[0]
        freq = sum(1 for b in bases if b != ref) / len(bases)
        if freq >= MIN_MUT_FREQ:
            positions.append((i + 1, ref, max(set(bases), key=bases.count), round(freq, 3)))
    return positions

# ----------------------------------------------------------
# MAIN PIPELINE
# ----------------------------------------------------------
if __name__ == "__main__":
    files = [f for f in os.listdir(CLEAN_DIR) if f.endswith(".fasta")]
    print(f"🧩 Found {len(files)} cleaned FASTA sequences. Splitting into chunks of {CHUNK_SIZE}...")

    # Split input files into chunks
    chunks = [files[i:i + CHUNK_SIZE] for i in range(0, len(files), CHUNK_SIZE)]
    chunk_paths = []

    for idx, group in enumerate(chunks):
        chunk_file = os.path.join(ALIGN_DIR, f"chunk_{idx}.fasta")
        if not os.path.exists(chunk_file):
            with open(chunk_file, "w") as f:
                for fname in group:
                    f.write(open(os.path.join(CLEAN_DIR, fname)).read())
        chunk_paths.append(chunk_file)

    print(f"📦 Created {len(chunk_paths)} chunks for alignment.")

    # Align chunks in parallel
    aligned_paths = [os.path.join(ALIGN_DIR, f"chunk_{i}_aligned.fasta") for i in range(len(chunk_paths))]
    print("⚙️ Running MAFFT alignments in parallel...")
    Parallel(n_jobs=THREADS)(
        delayed(run_mafft)(inp, out) for inp, out in zip(chunk_paths, aligned_paths)
    )

    # Merge all aligned chunks
    print("🔗 Combining all aligned chunks...")
    with open(FINAL_ALIGNMENT, "w") as outfile:
        for a in aligned_paths:
            if os.path.exists(a):
                outfile.write(open(a).read())

    print("✅ Combined alignment saved to:", FINAL_ALIGNMENT)

    # Detect mutation hotspots
    print("🧠 Analyzing mutation hotspots...")
    all_mutations = []

    for a in tqdm(aligned_paths):
        if not os.path.exists(a):
            continue
        positions = compute_hotspots(a)
        for pos, ref, alt, freq in positions:
            all_mutations.append({
                "position": pos,
                "ref_base": ref,
                "alt_base": alt,
                "frequency": freq,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })

    df = pd.DataFrame(all_mutations)
    output_csv = os.path.join(RESULT_DIR, "mutation_hotspots.csv")
    df.to_csv(output_csv, index=False)

    print(f"🔥 Mutation hotspot analysis complete — {len(df)} records saved.")
    print(f"📁 Output: {output_csv}")

