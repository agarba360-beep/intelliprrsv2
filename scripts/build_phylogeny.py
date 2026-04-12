import os
import pandas as pd
from Bio import Phylo, AlignIO
import matplotlib.pyplot as plt
from datetime import datetime
import subprocess

ALIGN_FILE = "/home/abubakar/intelliprrsv2/temp/alignment/prrsv_alignment.fasta"
TREE_FILE  = "/home/abubakar/intelliprrsv2/results/phylo/prrsv_tree.nwk"
PNG_FILE   = "/home/abubakar/intelliprrsv2/results/phylo/prrsv_tree.png"
LINEAGE_TABLE = "/home/abubakar/intelliprrsv2/results/phylo/lineage_table.csv"

# ------------------------------------------------------------
# 1. Build tree with FastTree
# ------------------------------------------------------------
def build_tree():
    print("🌳 Building phylogenetic tree with FastTree...")
    subprocess.run(["fasttree", "-nt", ALIGN_FILE], stdout=open(TREE_FILE, "w"), check=True)
    print(f"✅ Tree saved to {TREE_FILE}")

# ------------------------------------------------------------
# 2. Parse tree and generate lineage table
# ------------------------------------------------------------
def generate_lineages():
    print("🔍 Generating lineage table...")
    tree = Phylo.read(TREE_FILE, "newick")
    lineage_data = []

    for clade in tree.get_terminals():
        name = clade.name
        if not name:
            continue
        # Very simple lineage placeholder rule
        # (replace with your own reference clusters later)
        lineage = "Lineage-1" if "EU" in name.upper() else "Lineage-2"
        lineage_data.append({"accession": name, "lineage": lineage})

    df = pd.DataFrame(lineage_data)
    df["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df.to_csv(LINEAGE_TABLE, index=False)
    print(f"✅ Lineage table saved to {LINEAGE_TABLE}")

# ------------------------------------------------------------
# 3. Plot the tree (optional small overview)
# ------------------------------------------------------------
def plot_tree():
    tree = Phylo.read(TREE_FILE, "newick")
    fig = plt.figure(figsize=(12, 20))
    axes = fig.add_subplot(1, 1, 1)
    Phylo.draw(tree, do_show=False, axes=axes)
    plt.tight_layout()
    plt.savefig(PNG_FILE, dpi=300)
    print(f"🖼️  Tree image saved to {PNG_FILE}")

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    if not os.path.exists(ALIGN_FILE):
        print("❌ Alignment file not found. Run mutation_hotspot.py first.")
        return
    build_tree()
    generate_lineages()
    plot_tree()
    print("🎯 Phylogenetic analysis complete!")

if __name__ == "__main__":
    main()

