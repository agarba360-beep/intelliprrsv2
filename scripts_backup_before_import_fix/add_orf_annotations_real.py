#!/usr/bin/env python3
"""
🧬 IntelliPRRSV2 — Auto-Detect ORF Annotation Mapper
Chooses between VR-2332 (Type 2) or Lelystad (Type 1) automatically.
"""

import pandas as pd
from sqlalchemy import create_engine, text

# ---------------------------- DATABASE CONFIG ----------------------------
DB_CONFIG = {
    "host": "localhost",
    "user": "prrsvuser",
    "password": "PrrsvPass2026!",
    "database": "intelliprrsv2_db",
}
ENGINE_URL = (
    f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
)
engine = create_engine(ENGINE_URL)

# ---------------------------- REFERENCE MAPS -----------------------------
ORF_MAPS = {
    "VR2332": {
        "5' UTR": (1, 189),
        "ORF1a": (190, 7260),
        "ORF1b": (7261, 11860),
        "GP2 (ORF2a)": (11861, 12588),
        "E (ORF2b)": (12589, 12812),
        "GP3 (ORF3)": (12813, 13558),
        "GP4 (ORF4)": (13559, 14245),
        "GP5 (ORF5)": (14246, 14913),
        "M (ORF6)": (14914, 15421),
        "N (ORF7)": (15422, 15922),
        "3' UTR": (15923, 16100),
    },
    "Lelystad": {
        "5' UTR": (1, 190),
        "ORF1a": (191, 7265),
        "ORF1b": (7266, 11760),
        "GP2 (ORF2a)": (11761, 12517),
        "E (ORF2b)": (12518, 12749),
        "GP3 (ORF3)": (12750, 13477),
        "GP4 (ORF4)": (13478, 14215),
        "GP5 (ORF5)": (14216, 14920),
        "M (ORF6)": (14921, 15480),
        "N (ORF7)": (15481, 16020),
        "3' UTR": (16021, 16100),
    },
}

# ---------------------------- DETECTION LOGIC ----------------------------
def detect_reference(conn) -> str:
    """Infer PRRSV type from sequences table (metadata or length)."""
    try:
        df_seq = pd.read_sql(
            "SELECT accession, LENGTH(sequence) AS genome_len FROM sequences LIMIT 50", conn
        )
        if df_seq.empty:
            return "VR2332"

        # Check accession prefixes typical of Type 1
        eu_prefixes = ("AM", "AY", "AF", "DQ", "EU", "FN", "AB")
        na_prefixes = ("KR", "KF", "KJ", "EF", "GU", "MN")

        prefix = df_seq["accession"].astype(str).str[:2].mode().iloc[0]
        avg_len = df_seq["genome_len"].mean()

        if prefix in eu_prefixes or avg_len < 15500:
            ref = "Lelystad"
        elif prefix in na_prefixes or avg_len >= 15500:
            ref = "VR2332"
        else:
            ref = "VR2332"

        print(f"🧠 Auto-detected reference: {ref}  (avg len ≈ {avg_len:.0f} nt, prefix = {prefix})")
        return ref

    except Exception as e:
        print(f"⚠️ Detection failed → defaulting to VR2332: {e}")
        return "VR2332"

# ---------------------------- MAIN PIPELINE ------------------------------
with engine.begin() as conn:
    ref = detect_reference(conn)
    orf_map = ORF_MAPS[ref]

    df = pd.read_sql("SELECT id, position FROM mutation_hotspots", conn)
    print(f"✅ Loaded {len(df)} mutation records")

    def assign_orf(pos):
        if pd.isna(pos):
            return "Unknown"
        for orf, (start, end) in orf_map.items():
            if start <= pos <= end:
                return orf
        return "Intergenic"

    df["position"] = pd.to_numeric(df["position"], errors="coerce")
    df["orf_real"] = df["position"].apply(assign_orf)

    update_sql = text("UPDATE mutation_hotspots SET orf = :orf WHERE id = :id")
    for _, row in df.iterrows():
        conn.execute(update_sql, {"orf": row["orf_real"], "id": int(row["id"])})

print("✅ ORF mapping complete (automatic reference selection).")

