import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import xmltodict
import time

BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
OUT_RAW = "/home/abubakar/intelliprrsv2/data/raw/"
OUT_META = "/home/abubakar/intelliprrsv2/data/metadata/"
ACC_TRACKER = f"{OUT_META}/accessions.json"

os.makedirs(OUT_RAW, exist_ok=True)
os.makedirs(OUT_META, exist_ok=True)

# ----------------------------------------------------------
# Load previously fetched accessions
# ----------------------------------------------------------
if os.path.exists(ACC_TRACKER):
    with open(ACC_TRACKER) as f:
        fetched = set(json.load(f))
else:
    fetched = set()

# ----------------------------------------------------------
# Search GenBank for PRRSV sequences
# ----------------------------------------------------------
def search_prrsv():
    url = BASE_URL + "esearch.fcgi"
    params = {"db": "nuccore", "term": "PRRSV[Organism]", "retmax": 50000, "retmode": "json"}
    r = requests.get(url, params=params)
    r.raise_for_status()
    data = r.json()
    return data["esearchresult"]["idlist"]

# ----------------------------------------------------------
# Fetch one record (FASTA + metadata)
# ----------------------------------------------------------
def fetch_record(acc):
    try:
        fasta_url = BASE_URL + "efetch.fcgi"
        fasta_params = {"db": "nuccore", "id": acc, "rettype": "fasta", "retmode": "text"}
        fasta_data = requests.get(fasta_url, params=fasta_params, timeout=15).text

        if not fasta_data.strip().startswith(">"):
            return None  # skip invalid responses

        with open(os.path.join(OUT_RAW, f"{acc}.fasta"), "w") as f:
            f.write(fasta_data)

        # Fetch XML metadata
        xml_params = {"db": "nuccore", "id": acc, "rettype": "gb", "retmode": "xml"}
        xml_data = requests.get(fasta_url, params=xml_params, timeout=15).text
        meta = xmltodict.parse(xml_data)
        with open(os.path.join(OUT_META, f"{acc}.json"), "w") as f:
            json.dump(meta, f, indent=2)

        return acc

    except Exception as e:
        return None

# ----------------------------------------------------------
# MAIN PIPELINE
# ----------------------------------------------------------
if __name__ == "__main__":
    print("🔍 Searching for PRRSV sequences on NCBI...")
    ids = search_prrsv()
    new_ids = [i for i in ids if i not in fetched]
    print(f"🧬 Found {len(ids)} total, {len(new_ids)} new sequences to fetch.")

    # Fetch in parallel
    max_workers = 20
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_record, acc): acc for acc in new_ids}
        for future in tqdm(as_completed(futures), total=len(futures)):
            acc = future.result()
            if acc:
                fetched.add(acc)
                results.append(acc)
                if len(results) % 100 == 0:
                    # checkpoint
                    with open(ACC_TRACKER, "w") as f:
                        json.dump(list(fetched), f, indent=2)

    # Save tracker
    with open(ACC_TRACKER, "w") as f:
        json.dump(list(fetched), f, indent=2)

    print(f"✅ Completed — {len(results)} new sequences fetched.")

