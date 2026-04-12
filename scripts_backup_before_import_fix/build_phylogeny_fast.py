import os, subprocess, pandas as pd
from datetime import datetime

ALIGNMENT = "/home/abubakar/intelliprrsv2/temp/prrsv_alignment.fasta"
TREE_OUT = "/home/abubakar/intelliprrsv2/results/prrsv_tree_fast.nwk"
LINEAGE_OUT = "/home/abubakar/intelliprrsv2/results/lineage_table_fast.csv"

os.makedirs("/home/abubakar/intelliprrsv2/results", exist_ok=True)

# ----------------------------------------
# Step 1: Build tree using FastTree
# ----------------------------------------
print("🌳 Building phylogenetic tree with FastTree...")
subprocess.run(["FastTree", "-nt", ALIGNMENT], stdout=open(TREE_OUT, "w"))
print(f"✅ Tree saved → {TREE_OUT}")

# ----------------------------------------
# Step 2: Assign simple lineages
# ----------------------------------------
print("🧬 Assigning lineages...")
seq_headers = [l.strip()[1:] for l in open(ALIGNMENT) if l.startswith(">")]

lineage_data = []
for i, header in enumerate(seq_headers):
    lineage = f"L{i % 5 + 1}"  # Assign 5 pseudo-lineages for grouping
    lineage_data.append({
        "accession": header,
        "assigned_lineage": lineage,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

df = pd.DataFrame(lineage_data)
df.to_csv(LINEAGE_OUT, index=False)
print(f"✅ Lineage assignments saved → {LINEAGE_OUT}")

