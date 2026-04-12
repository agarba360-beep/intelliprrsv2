import os
import json
import time
import mysql.connector
from datetime import datetime
from tqdm import tqdm
from Bio import Entrez, SeqIO

# ----------------------------------------
# CONFIGURATION
# ----------------------------------------
Entrez.email = "your_email@example.com"  # use your NCBI-registered email
DATA_DIR = "/home/abubakar/intelliprrsv2/data/raw/"
ACCESSION_FILE = "/home/abubakar/intelliprrsv2/data/metadata/accessions.json"

# MySQL connection
conn = mysql.connector.connect(
    host="localhost",
    user="prrsvuser",
    password="PrrsvPass2026!",
    database="intelliprrsv2_db"
)
cursor = conn.cursor()

# Ensure output folder exists
os.makedirs(DATA_DIR, exist_ok=True)

# ----------------------------------------
# LOAD ACCESSIONS
# ----------------------------------------
if not os.path.exists(ACCESSION_FILE):
    print(f"❌ Accession list not found: {ACCESSION_FILE}")
    exit()

with open(ACCESSION_FILE) as f:
    accessions = json.load(f)

print(f"📥 Total accessions to fetch: {len(accessions)}")

# ----------------------------------------
# FETCH AND SAVE TO MYSQL
# ----------------------------------------
for acc in tqdm(accessions, desc="Fetching sequences from NCBI"):
    fasta_path = os.path.join(DATA_DIR, f"{acc}.fasta")

    # Skip if already downloaded
    if os.path.exists(fasta_path):
        continue

    try:
        handle = Entrez.efetch(db="nucleotide", id=acc, rettype="fasta", retmode="text")
        record = SeqIO.read(handle, "fasta")
        handle.close()

        organism = record.description.split(" ")[1] if " " in record.description else "Unknown"
        seq_str = str(record.seq)

        # Save locally
        SeqIO.write(record, fasta_path, "fasta")

        # Insert into MySQL
        cursor.execute("""
            INSERT INTO sequences (accession, organism, collection_date, country, sequence)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE sequence = VALUES(sequence)
        """, (acc, organism, None, None, seq_str))

        conn.commit()
        time.sleep(0.2)

    except Exception as e:
        print(f"⚠️ Error fetching {acc}: {e}")
        time.sleep(2)

# ----------------------------------------
# CLEANUP
# ----------------------------------------
cursor.close()
conn.close()

print("✅ Fetching and MySQL upload complete!")

