#!/usr/bin/env python3
"""
fetch_to_mysql_parallel_safe.py
Optimized PRRSV fetcher — multi-threaded, resume-safe, and rate-limit friendly.
Fetches up to 1000 PRRSV sequences from NCBI and stores them in MySQL.
"""

import os
import time
import random
import mysql.connector
from concurrent.futures import ThreadPoolExecutor, as_completed
from Bio import Entrez, SeqIO
from tqdm import tqdm

# ──────────────────────────────────────────────
# ⚙️ CONFIGURATION
# ──────────────────────────────────────────────
MAX_FETCH = 1000          # test limit
MAX_WORKERS = 4           # safe number of threads for NCBI
API_DELAY = 0.35          # seconds between requests (per thread)
MAX_RETRIES = 3           # retry attempts per sequence

Entrez.email = "your_email@example.com"
Entrez.api_key = "YOUR_NCBI_API_KEY"  # optional but recommended

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "YOUR_DB_PASSWORD",
    "database": "intelliprrsv2_db",
}

# ──────────────────────────────────────────────
# 🧩 DATABASE FUNCTIONS
# ──────────────────────────────────────────────
def connect_db():
    """Connect to MySQL database."""
    return mysql.connector.connect(**DB_CONFIG)

def create_table_if_not_exists(cursor):
    """Ensure PRRSV table exists."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS prrsv_sequences (
            accession VARCHAR(50) PRIMARY KEY,
            sequence LONGTEXT,
            organism VARCHAR(255),
            collection_date VARCHAR(255),
            country VARCHAR(255)
        )
    """)

def get_existing_accessions(cursor):
    """Fetch list of accessions already in DB."""
    cursor.execute("SELECT accession FROM prrsv_sequences")
    return {row[0] for row in cursor.fetchall()}

# ──────────────────────────────────────────────
# 🔍 FETCH FUNCTIONS
# ──────────────────────────────────────────────
def fetch_accessions(term="PRRSV[Organism] AND complete genome"):
    """Get up to MAX_FETCH accession IDs."""
    handle = Entrez.esearch(db="nucleotide", term=term, retmax=MAX_FETCH)
    records = Entrez.read(handle)
    handle.close()
    return records["IdList"]

def fetch_sequence(accession):
    """Fetch a single sequence record with retries."""
    for attempt in range(MAX_RETRIES):
        try:
            handle = Entrez.efetch(db="nucleotide", id=accession, rettype="fasta", retmode="text")
            records = list(SeqIO.parse(handle, "fasta"))
            handle.close()
            if not records:
                return None, None
            time.sleep(API_DELAY + random.uniform(0.05, 0.15))
            return str(records[0].id), str(records[0].seq)
        except Exception as e:
            time.sleep(2 + attempt * 2)
            if attempt == MAX_RETRIES - 1:
                print(f"⚠️ Failed to fetch {accession}: {e}")
    return None, None

def process_accession(acc, existing):
    """Fetch and prepare one sequence if not already fetched."""
    if acc in existing:
        return None
    acc_id, seq = fetch_sequence(acc)
    if not seq:
        return None
    return (acc_id, seq, "PRRSV", "Unknown", "Unknown")

# ──────────────────────────────────────────────
# 🚀 MAIN EXECUTION
# ──────────────────────────────────────────────
if __name__ == "__main__":
    print(f"📥 Fetching up to {MAX_FETCH} PRRSV sequences (multi-threaded, resume-safe)...")

    db = connect_db()
    cursor = db.cursor()
    create_table_if_not_exists(cursor)

    accessions = fetch_accessions()
    existing = get_existing_accessions(cursor)

    remaining = [a for a in accessions if a not in existing]
    print(f"⏩ Resuming — {len(existing)} already fetched, {len(remaining)} remaining")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_accession, acc, existing): acc for acc in remaining}

        for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching NCBI Sequences"):
            result = future.result()
            if result:
                results.append(result)

                # Insert immediately to keep progress saved
                cursor.execute("""
                    REPLACE INTO prrsv_sequences (accession, sequence, organism, collection_date, country)
                    VALUES (%s, %s, %s, %s, %s)
                """, result)
                db.commit()

    db.close()
    print(f"✅ Done! {len(results)} new sequences saved to MySQL successfully.")

