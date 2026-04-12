import os
import requests
import xmltodict
import time
from tqdm import tqdm
import json
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# -------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------
BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
OUT_RAW = "/home/abubakar/intelliprrsv2/data/raw/"
OUT_META = "/home/abubakar/intelliprrsv2/data/metadata/"
ACC_TRACKER = "/home/abubakar/intelliprrsv2/data/metadata/accessions.json"
LOG_FILE = "/home/abubakar/intelliprrsv2/logs/fetch_error.log"

BATCH_SIZE = 100           # sequences per batch (adjustable)
SLEEP_BETWEEN_BATCH = 1    # seconds between batches
MAX_BATCHES_PER_RUN = 50   # limit per run (~5,000 sequences)
TEST_MODE = False           # Set True for quick testing (only 50 seqs)

# -------------------------------------------------------
# PREPARE FOLDERS
# -------------------------------------------------------
for path in [OUT_RAW, OUT_META, os.path.dirname(ACC_TRACKER)]:
    os.makedirs(path, exist_ok=True)

# -------------------------------------------------------
# LOGGING SETUP
# -------------------------------------------------------
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# -------------------------------------------------------
# RETRY SESSION (auto retries if NCBI fails)
# -------------------------------------------------------
session = requests.Session()
retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retry))

# -------------------------------------------------------
# LOAD ALREADY-FETCHED ACCESSIONS
# -------------------------------------------------------
if os.path.exists(ACC_TRACKER):
    with open(ACC_TRACKER, "r") as f:
        fetched = set(json.load(f))
else:
    fetched = set()

# -------------------------------------------------------
# STEP 1: SEARCH GENBANK FOR PRRSV SEQUENCES
# -------------------------------------------------------
def search_prrsv():
    """Search NCBI for all PRRSV nucleotide accessions."""
    url = BASE_URL + "esearch.fcgi"
    params = {
        "db": "nuccore",
        "term": "PRRSV[Organism]",
        "retmax": 50000,
        "retmode": "json"
    }
    r = session.get(url, params=params)
    data = r.json()
    return data["esearchresult"]["idlist"]

# -------------------------------------------------------
# STEP 2: FETCH SEQUENCES + METADATA (in batch)
# -------------------------------------------------------
def fetch_batch(batch_ids):
    """Fetch FASTA and XML metadata for a batch of accessions."""
    ids = ",".join(batch_ids)

    # Fetch FASTA
    fasta_params = {"db": "nuccore", "id": ids, "rettype": "fasta", "retmode": "text"}
    fasta_data = session.get(BASE_URL + "efetch.fcgi", params=fasta_params).text
    entries = fasta_data.strip().split(">")[1:]
    for entry in entries:
        try:
            header, seq = entry.split("\n", 1)
            acc = header.split()[0]
            with open(OUT_RAW + f"{acc}.fasta", "w") as f:
                f.write(">" + header + "\n" + seq)
        except Exception as e:
            logging.error(f"Error saving FASTA for acc {acc}: {e}")

    # Fetch Metadata (XML)
    xml_params = {"db": "nuccore", "id": ids, "rettype": "gb", "retmode": "xml"}
    xml_data = session.get(BASE_URL + "efetch.fcgi", params=xml_params).text
    xml_blocks = xml_data.split("</GBSeq>")
    for block in xml_blocks:
        if "<GBSeq_locus>" in block:
            block = block + "</GBSeq>"
            try:
                acc = block.split("<GBSeq_locus>")[1].split("</GBSeq_locus>")[0]
                meta = xmltodict.parse("<GBSeq>" + block + "</GBSeq>")
                with open(OUT_META + f"{acc}.json", "w") as f:
                    json.dump(meta, f, indent=2)
            except Exception as e:
                logging.error(f"Error parsing metadata for {acc}: {e}")

# -------------------------------------------------------
# MAIN PIPELINE
# -------------------------------------------------------
def main():
    print("🔍 Searching NCBI for PRRSV sequences...")
    ids = search_prrsv()
    print(f"Found {len(ids)} total sequences.")

    new_ids = [i for i in ids if i not in fetched]
    if TEST_MODE:
        new_ids = new_ids[:50]  # limit for testing
    print(f"New sequences to fetch: {len(new_ids)}")

    total_batches = len(new_ids) // BATCH_SIZE
    batches_to_run = min(total_batches, MAX_BATCHES_PER_RUN)
    print(f"⚙️ Fetching {batches_to_run * BATCH_SIZE} sequences this run...")

    for i in tqdm(range(batches_to_run)):
        start = i * BATCH_SIZE
        end = start + BATCH_SIZE
        batch = new_ids[start:end]
        try:
            fetch_batch(batch)
            fetched.update(batch)
            with open(ACC_TRACKER, "w") as f:
                json.dump(list(fetched), f, indent=2)
            time.sleep(SLEEP_BETWEEN_BATCH)
        except Exception as e:
            logging.error(f"Error fetching batch {i}: {e}")
            continue

    print("✅ Run complete. Tracker updated.")
    logging.info("✅ Run complete.")

if __name__ == "__main__":
    main()

