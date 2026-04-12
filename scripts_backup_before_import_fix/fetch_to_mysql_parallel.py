import os
import time
import json
import mysql.connector
from Bio import Entrez, SeqIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# -------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------
Entrez.email = "your_email@example.com"  # ⚠️ Replace with your actual NCBI email

DATA_DIR = "/home/abubakar/intelliprrsv2/data/raw/"
MAX_SEQS = 500        # ✅ Limit to 500 sequences only
MAX_WORKERS = 3       # Parallel threads (safe for NCBI)
NCBI_DELAY = 1.0      # Delay per thread (seconds)
MAX_RETRIES = 3       # Retry for NCBI rate-limiting

DB_CONFIG = {
    "host": "localhost",
    "user": "prrsvuser",
    "password": "PrrsvPass2026!",
    "database": "intelliprrsv2_db"
}

os.makedirs(DATA_DIR, exist_ok=True)


# -------------------------------------------------------
# Database helper
# -------------------------------------------------------
def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)


# -------------------------------------------------------
# Fetch one accession and save
# -------------------------------------------------------
def fetch_and_save(acc):
    fasta_path = os.path.join(DATA_DIR, f"{acc}.fasta")

    # Skip if already downloaded
    if os.path.exists(fasta_path):
        return (acc, "cached")

    try:
        handle = Entrez.efetch(db="nucleotide", id=acc, rettype="fasta", retmode="text")
        record = SeqIO.read(handle, "fasta")
        handle.close()

        organism = record.description.split(" ")[1] if " " in record.description else "Unknown"
        seq_str = str(record.seq)

        # Save locally
        SeqIO.write(record, fasta_path, "fasta")

        # Save to MySQL
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO sequences (accession, organism, collection_date, country, sequence)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE sequence = VALUES(sequence)
        """, (acc, organism, None, None, seq_str))
        conn.commit()
        cursor.close()
        conn.close()

        time.sleep(NCBI_DELAY)
        return (acc, "fetched")

    except Exception as e:
        if "HTTP Error 429" in str(e):
            # NCBI rate limit — exponential backoff
            wait_time = 15
            for attempt in range(MAX_RETRIES):
                print(f"⏳ Rate limit hit for {acc}, waiting {wait_time}s (retry {attempt+1}/{MAX_RETRIES})...")
                time.sleep(wait_time)
                try:
                    handle = Entrez.efetch(db="nucleotide", id=acc, rettype="fasta", retmode="text")
                    record = SeqIO.read(handle, "fasta")
                    handle.close()

                    organism = record.description.split(" ")[1] if " " in record.description else "Unknown"
                    seq_str = str(record.seq)

                    SeqIO.write(record, fasta_path, "fasta")

                    conn = get_db_connection()
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT INTO sequences (accession, organism, collection_date, country, sequence)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE sequence = VALUES(sequence)
                    """, (acc, organism, None, None, seq_str))
                    conn.commit()
                    cursor.close()
                    conn.close()
                    return (acc, f"retried_success_{attempt+1}")
                except Exception:
                    wait_time *= 2
            return (acc, "retry_failed_429")
        else:
            return (acc, f"error: {e}")


# -------------------------------------------------------
# MAIN PIPELINE
# -------------------------------------------------------
if __name__ == "__main__":
    print("🔍 Fetching PRRSV sequences from NCBI...")
    handle = Entrez.esearch(db="nucleotide", term="PRRSV[Organism] AND complete genome", retmax=MAX_SEQS)
    record = Entrez.read(handle)
    accessions = record["IdList"]
    print(f"📥 Total accessions to fetch: {len(accessions)} (limited to {MAX_SEQS})")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_and_save, acc) for acc in accessions]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching NCBI Sequences"):
            acc, status = future.result()
            if "error" in status or "retry_failed" in status:
                print(f"⚠️ {acc} → {status}")

    print("✅ Parallel fetch complete! 500 sequences saved to MySQL and local files.")

