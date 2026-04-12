from Bio import Entrez, SeqIO
import os, time, json
from tqdm import tqdm

# --------------------------------------------
# CONFIGURATION
# --------------------------------------------
Entrez.email = "your_email@example.com"  # (use your real email for NCBI)
RAW_DIR = "/home/abubakar/intelliprrsv2/data/raw/"
META_DIR = "/home/abubakar/intelliprrsv2/data/metadata/"
ACCESSION_FILE = os.path.join(META_DIR, "accessions.json")

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(META_DIR, exist_ok=True)

# --------------------------------------------
# FETCH ACCESSIONS
# --------------------------------------------
def get_accessions():
    """Get all PRRSV nucleotide accessions from NCBI."""
    print("🔎 Searching NCBI for PRRSV nucleotide sequences...")
    handle = Entrez.esearch(
        db="nucleotide",
        term="PRRSV[Organism]",
        retmax=50000
    )
    record = Entrez.read(handle)
    ids = record["IdList"]
    with open(ACCESSION_FILE, "w") as f:
        json.dump(ids, f, indent=2)
    print(f"✅ Found {len(ids)} total accessions.")
    return ids

# --------------------------------------------
# FETCH ONE SEQUENCE
# --------------------------------------------
def fetch_sequence(acc):
    """Fetch and save one FASTA record from NCBI."""
    fasta_path = os.path.join(RAW_DIR, f"{acc}.fasta")
    if os.path.exists(fasta_path):
        return None  # skip already fetched

    for attempt in range(3):
        try:
            handle = Entrez.efetch(db="nucleotide", id=acc, rettype="fasta", retmode="text")
            record = handle.read()
            with open(fasta_path, "w") as f:
                f.write(record)
            time.sleep(0.3)
            return acc
        except Exception as e:
            print(f"⚠️ Retry {attempt+1} for {acc} due to {e}")
            time.sleep(2)
    return None

# --------------------------------------------
# MAIN EXECUTION
# --------------------------------------------
if __name__ == "__main__":
    print("🚀 Starting PRRSV data fetch...")
    accessions = get_accessions()
    existing = {f.replace(".fasta", "") for f in os.listdir(RAW_DIR)}
    new = [a for a in accessions if a not in existing]

    print(f"📥 New sequences to fetch: {len(new)}")
    for acc in tqdm(new):
        fetch_sequence(acc)

    print("✅ Fetching complete! All sequences stored in data/raw/")

