#!/usr/bin/env python3
import os
import json
from sqlalchemy import create_engine, text

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

META_DIR = "/home/abubakar/intelliprrsv2/data/metadata"


def safe_get(d, path, default=None):
    cur = d
    for p in path:
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return default
    return cur


def extract_qualifiers(gbseq):
    """
    Extract /country, /collection_date, etc from GBSeq_feature-table source qualifiers.
    """
    qualifiers = {}
    ft = gbseq.get("GBSeq_feature-table", {})
    feats = ft.get("GBFeature", [])
    if isinstance(feats, dict):
        feats = [feats]

    for feat in feats:
        if feat.get("GBFeature_key") == "source":
            quals = feat.get("GBFeature_quals", {}).get("GBQualifier", [])
            if isinstance(quals, dict):
                quals = [quals]
            for q in quals:
                name = q.get("GBQualifier_name")
                val = q.get("GBQualifier_value")
                if name and val:
                    qualifiers[name] = str(val)
            break

    return qualifiers


def normalize_country(raw):
    if not raw:
        return None
    raw = str(raw).strip()
    if ":" in raw:
        raw = raw.split(":", 1)[0].strip()
    if "," in raw:
        raw = raw.split(",", 1)[0].strip()
    return raw


def main():
    print("🔄 Backfilling sequences.country + sequences.collection_date from metadata JSON...")

    files = [f for f in os.listdir(META_DIR) if f.endswith(".json")]
    print(f"📦 Found metadata JSON files: {len(files):,}")

    processed = 0
    skipped = 0

    with engine.begin() as conn:
        for i, fn in enumerate(files, 1):
            fp = os.path.join(META_DIR, fn)

            # ✅ UID numeric id comes from JSON filename (e.g. 1001604658.json)
            uid = os.path.splitext(fn)[0]
            if not uid.isdigit():
                skipped += 1
                continue

            try:
                with open(fp, "r") as f:
                    meta = json.load(f)
            except Exception:
                skipped += 1
                continue

            gbseq = safe_get(meta, ["GBSet", "GBSeq"], {})
            organism = gbseq.get("GBSeq_organism")

            quals = extract_qualifiers(gbseq)
            raw_country = quals.get("country") or quals.get("geo_loc_name")
            raw_date = quals.get("collection_date")

            country = normalize_country(raw_country)

            # ✅ THIS is the key: match sequences.accession with UID
            conn.execute(
                text("""
                    UPDATE sequences
                    SET
                      country = COALESCE(country, :country),
                      collection_date = COALESCE(collection_date, :collection_date),
                      organism = COALESCE(organism, :organism)
                    WHERE accession = :uid
                """),
                {
                    "uid": uid,
                    "country": country,
                    "collection_date": raw_date,
                    "organism": organism,
                }
            )

            processed += 1

            if i % 1000 == 0:
                print(f"✅ Processed {i:,} metadata files...")

    print("===================================")
    print(f"✅ Completed processing {processed:,} files.")
    print(f"⚠️ Skipped unreadable/invalid: {skipped:,}")
    print("===================================")


if __name__ == "__main__":
    main()

