import os, requests, xmltodict, time, json
from datetime import datetime
from tqdm import tqdm

# ----------------------------------------
# CONFIGURATION
# ----------------------------------------
BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
OUT_RAW = "/home/abubakar/intelliprrsv2/data/raw/"
OUT_META = "/home/abubakar/intelliprrsv2/data/metadata/"
ACC_TRACKER = "/home/abubakar/intelliprrsv2/data/metadata/accessions.json"
MAX_FETCH = 300   # limit to 300 sequences for fast runtime
REQUEST_DELAY = 0.25  # seconds between API calls

os.makedirs(OUT_RAW, exist_ok=True)
os.makedirs(OUT_META, exist_ok=True)

# ----------------------------------------
# Load already-fetched accession IDs
# ----------------------------------------
if os.path.exists(ACC_TRACKER):
    with open(ACC_TRACKER, "r") as f:
        fetched = set(json.load(f))
else:
    fetched = set()

# ----------------------------------------
# Step 1: Search GenBank for PRRSV
# ----------------------------------------
def search_prrsv():
    url = BASE_URL + "esearch.fcgi"
    params = {
        "db": "nuccore",
        "term": "PRRSV[Organism] AND complete genome",
        "retmax": MAX_FETCH,
        "sort": "datemodified",
        "retmode": "json"
    }
    r = requests.get(url, params=params, timeout=20)
    data = r.json()
    return data["esearchresult"]["idlist"]

# ----------------------------------------
# Step 2: Fetch one sequence + metadata
# ----------------------------------------
def fetch_record(acc):
    try:
        # FASTA
        fasta_params = {"db": "nuccore", "id": acc, "rettype": "fasta", "retmode": "text"}
        fasta = requests.get(BASE_URL + "efetch.fcgi", params=fasta_params, timeout=30).text
        with open(os.path.join(OUT_RAW, f"{acc}.fasta"), "w") as f:
            f.write(fasta)

        # Metadata XML
        xml_params = {"db": "nuccore", "id": acc, "rettype": "gb", "retmode": "xml"}
        xml_data = requests.get(BASE_URL + "efetch.fcgi", params=xml_params, timeout=30).text
        meta = xmltodict.parse(xml_data)
        with open(os.path.join(OUT_META, f"{acc}.json"), "w") as f:
            json.dump(meta, f, indent=2)

        return True
    except Exception as e:
        print(f"⚠️ {acc} failed: {e}")
        return False

# ----------------------------------------
# MAIN
# ----------------------------------------
def main():
    print(f"🔍 Searching NCBI for PRRSV (max {MAX_FETCH}) ...")
    ids = search_prrsv()
    new_ids = [i for i in ids if i not in fetched]
    print(f"🧬 New sequences to fetch: {len(new_ids)}")

    for acc in tqdm(new_ids, desc="Fetching PRRSV"):
        if fetch_record(acc):
            fetched.add(acc)
        time.sleep(REQUEST_DELAY)

    with open(ACC_TRACKER, "w") as f:
        json.dump(list(fetched), f, indent=2)
    print("✅ FAST fetch complete.")

if __name__ == "__main__":
    main()

