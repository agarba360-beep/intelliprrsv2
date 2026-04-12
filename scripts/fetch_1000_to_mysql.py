import os
import time
import mysql.connector
from Bio import Entrez
from tqdm import tqdm

# ----------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------
Entrez.email = "your_email@example.com"  # 👈 Replace with your actual email
MAX_SEQS = 1000          # Fetch only 1000 sequences for testing
RETRY_DELAY = 10         # Retry delay in seconds if rate limited
BATCH_SIZE = 50          # Fetch 50 sequences at once
FETCH_DELAY = 1          # Small delay between batches

DB_CONFIG = {
    "host": "localhost",
    "user": "prrsvuser",
    "password": "PrrsvPass2026!",
    "database": "intelliprrsv2_db",
}

# ----------------------------------------------------------
# DATABASE SETUP
# ----------------------------------------------------------
print("🧩 Connecting to MySQL...")
try:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sequences (
        accession VARCHAR(50) PRIMARY KEY,
        organism VARCHAR(255),
        country VARCHAR(100),
        collection_date VARCHAR(100),
        sequence LONGTEXT,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    conn.commit()
    print("✅ Database connected and ready.")
except mysql.connector.Error as e:
    print(f"❌ MySQL connection failed: {e}")
    exit(1)

# ----------------------------------------------------------
# NCBI FETCHING — ACCESSION LIST
# ----------------------------------------------------------
print("🔍 Searching NCBI for PRRSV sequences...")
try:
    handle = Entrez.esearch(db="nucleotide", term="PRRSV[Organism] AND complete genome", retmax=MAX_SEQS)
    record = Entrez.read(handle)
    accessions = record.get("IdList", [])
except Exception as e:
    print(f"❌ Failed to fetch accession list: {e}")
    conn.close()
    exit(1)

print(f"🧬 Found {len(accessions)} PRRSV sequences (limited to {MAX_SEQS})")

# ----------------------------------------------------------
# SKIP ALREADY FETCHED
# ----------------------------------------------------------
cursor.execute("SELECT accession FROM sequences;")
existing = {r[0] for r in cursor.fetchall()}
accessions = [a for a in accessions if a not in existing]
print(f"🔁 Skipping {len(existing)} already-fetched sequences.")
print(f"➡️  Fetching {len(accessions)} new sequences...")

# ----------------------------------------------------------
# BATCH FETCH FUNCTION
# ----------------------------------------------------------
def fetch_batch(acc_list):
    """Fetch a batch of accessions and insert them into MySQL."""
    ids = ",".join(acc_list)
    for attempt in range(3):
        try:
            handle = Entrez.efetch(db="nucleotide", id=ids, rettype="gb", retmode="xml")
            records = Entrez.read(handle)
            count = 0
            for record in records:
                acc = record.get("GBSeq_primary-accession", "")
                organism = record.get("GBSeq_organism", "Unknown")
                sequence = record.get("GBSeq_sequence", "").upper()
                country, date = "Unknown", "Unknown"

                for f in record.get("GBSeq_feature-table", []):
                    for q in f.get("GBFeature_quals", []):
                        if q["GBQualifier_name"] == "country":
                            country = q["GBQualifier_value"]
                        elif q["GBQualifier_name"] == "collection_date":
                            date = q["GBQualifier_value"]

                cursor.execute("""
                    INSERT INTO sequences (accession, organism, country, collection_date, sequence)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE sequence=%s;
                """, (acc, organism, country, date, sequence, sequence))
                count += 1

            conn.commit()
            return count

        except Exception as e:
            print(f"⚠️ Batch of {len(acc_list)} failed (Attempt {attempt+1}/3): {e}")
            time.sleep(RETRY_DELAY)
    return 0

# ----------------------------------------------------------
# MAIN LOOP — FETCH IN BATCHES
# ----------------------------------------------------------
success = 0
for i in tqdm(range(0, len(accessions), BATCH_SIZE), desc="📡 Batch Fetching"):
    batch = accessions[i:i + BATCH_SIZE]
    success += fetch_batch(batch)
    time.sleep(FETCH_DELAY)

# ----------------------------------------------------------
# CLEANUP
# ----------------------------------------------------------
cursor.close()
conn.close()
print(f"✅ Fetch complete — {success}/{len(accessions)} new sequences saved successfully.")

