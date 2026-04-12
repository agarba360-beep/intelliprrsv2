import os
import pandas as pd
import numpy as np
from Bio import SeqIO, pairwise2
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score
from tqdm import tqdm
import joblib
from datetime import datetime

# ----------------------------------------------------------
# PATH CONFIGURATION
# ----------------------------------------------------------
MUTATION_FILE = "/home/abubakar/intelliprrsv2/results/hotspots/mutation_hotspots.csv"
LINEAGE_FILE  = "/home/abubakar/intelliprrsv2/results/phylo/real_lineage_table.csv"
VACCINE_FILE  = "/home/abubakar/intelliprrsv2/reference/vaccines/vaccine_refs.csv"
VACCINE_DIR   = "/home/abubakar/intelliprrsv2/reference/vaccines/"
CLEAN_DIR     = "/home/abubakar/intelliprrsv2/data/clean/"
MODEL_FILE    = "/home/abubakar/intelliprrsv2/models/vaccine_escape_model_v2.pkl"
OUTPUT_FILE   = "/home/abubakar/intelliprrsv2/results/ai/vaccine_escape_predictions_v2.csv"

os.makedirs(os.path.dirname(MODEL_FILE), exist_ok=True)
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

# ----------------------------------------------------------
# LOAD INPUT DATA
# ----------------------------------------------------------
print("📥 Loading datasets...")
mut_df = pd.read_csv(MUTATION_FILE)
lin_df = pd.read_csv(LINEAGE_FILE)
vac_df = pd.read_csv(VACCINE_FILE)

print(f"✅ Loaded: {len(mut_df)} mutations | {len(lin_df)} lineages | {len(vac_df)} vaccine references")

# ----------------------------------------------------------
# FEATURE 1: Approximate Mutation Load (Global)
# ----------------------------------------------------------
# Since your mutation file doesn’t contain sequence IDs,
# we’ll use a global mutation density metric for all isolates.
global_mut_count = len(mut_df)
lin_df["mutation_count"] = global_mut_count / len(lin_df)

# ----------------------------------------------------------
# FEATURE 2: Genetic Similarity to Vaccine Strains
# ----------------------------------------------------------
def compute_identity(seq1, seq2):
    """Compute nucleotide identity between two sequences."""
    if len(seq1) == 0 or len(seq2) == 0:
        return 0
    score = pairwise2.align.globalxx(seq1, seq2, one_alignment_only=True, score_only=True)
    return score / max(len(seq1), len(seq2))

# Load vaccine sequences
print("🧬 Loading vaccine reference genomes...")
vaccine_records = {}
for _, row in vac_df.iterrows():
    fasta_path = os.path.join(VACCINE_DIR, f"{row['vaccine_name']}_{row['accession']}.fasta")
    if os.path.exists(fasta_path):
        record = next(SeqIO.parse(fasta_path, "fasta"))
        vaccine_records[row['vaccine_name']] = {
            "seq": str(record.seq),
            "lineage": row['lineage'],
            "region": row['region']
        }
print(f"✅ Loaded {len(vaccine_records)} vaccine genomes")

# Compare each cleaned isolate to all vaccine genomes
print("🔬 Computing similarities between isolates and vaccine strains...")
similarities = []
for f in tqdm(os.listdir(CLEAN_DIR), desc="Comparing sequences"):
    if not f.endswith(".fasta"):
        continue
    acc = f.replace(".fasta", "")
    rec = next(SeqIO.parse(os.path.join(CLEAN_DIR, f), "fasta"))
    seq = str(rec.seq)
    for vac, info in vaccine_records.items():
        ident = compute_identity(seq, info["seq"])
        similarities.append({
            "accession": acc,
            "vaccine_name": vac,
            "vaccine_lineage": info["lineage"],
            "vaccine_region": info["region"],
            "vaccine_similarity(%)": round(ident * 100, 2)
        })

sim_df = pd.DataFrame(similarities)
print(f"✅ Computed {len(sim_df)} isolate–vaccine similarity pairs")

# Keep only best vaccine match per isolate
best_sim = sim_df.loc[sim_df.groupby("accession")["vaccine_similarity(%)"].idxmax()]

# Merge with lineage data
data = lin_df.merge(best_sim, on="accession", how="left")

# ----------------------------------------------------------
# LABEL GENERATION (Escape = 1 if <70% similarity)
# ----------------------------------------------------------
data["escape_status"] = data["vaccine_similarity(%)"].apply(lambda x: 1 if x < 70 else 0)

# ----------------------------------------------------------
# MACHINE LEARNING MODEL
# ----------------------------------------------------------
features = ["mutation_count", "similarity(%)", "vaccine_similarity(%)"]
X = data[features].fillna(0)
y = data["escape_status"]

if len(X) < 5:
    print("⚠️ Not enough data points for training. Exiting gracefully.")
    exit()

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
model = RandomForestClassifier(n_estimators=150, random_state=42)
model.fit(X_train, y_train)

preds = model.predict(X_test)
probs = model.predict_proba(X_test)[:, 1]

acc = accuracy_score(y_test, preds)
auc = roc_auc_score(y_test, probs)
print(f"✅ Model trained successfully | Accuracy: {acc:.3f} | AUC: {auc:.3f}")

# ----------------------------------------------------------
# SAVE MODEL AND RESULTS
# ----------------------------------------------------------
joblib.dump(model, MODEL_FILE)
print(f"💾 Model saved → {MODEL_FILE}")

data["escape_probability"] = model.predict_proba(X)[:, 1]
data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

data.to_csv(OUTPUT_FILE, index=False)
print(f"📊 Predictions saved → {OUTPUT_FILE}")

print("\n🎯 Vaccine escape prediction complete.")

