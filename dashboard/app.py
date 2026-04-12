#!/usr/bin/env python3
# ============================================================
# 🌌 IntelliPRRSV2 — Neon Bioinformatics Dashboard (Optimized v2.4)
# ============================================================

import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
from io import StringIO
from Bio import Phylo
import matplotlib.pyplot as plt
import time
from datetime import datetime
import json
import networkx as nx

# ============================================================
# ⚙️ DATABASE CONFIGURATION
# ============================================================
DB_CONFIG = {
    "host": "localhost",
    "user": "prrsvuser",
    "password": "PrrsvPass2026!",
    "database": "intelliprrsv2_db"
}
ENGINE_URL = (
    f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
)
engine = create_engine(ENGINE_URL)
# ============================================================
# 🧬 PIPELINE HEALTH — AUTHORITATIVE SOURCE
# ============================================================

def get_pipeline_health():
    with engine.connect() as conn:
        return conn.execute(
            text("""
                SELECT run_id, start_time, status
                FROM pipeline_runs
                WHERE status = 'completed'
                ORDER BY run_id DESC
                LIMIT 1
            """)
        ).fetchone()

PIPELINE_RUN = get_pipeline_health()
PIPELINE_RUN_ID = PIPELINE_RUN[0]
PIPELINE_RUN_TS = PIPELINE_RUN[1]


# ============================================================
# 🕒 PIPELINE RUN TIMESTAMP (AUTHORITATIVE)
# ============================================================

# (Removed PIPELINE_RUN_TS / PIPELINE_RUN_STR logic)


# ============================================================
# 🧭 STREAMLIT PAGE SETUP
# ============================================================

st.set_page_config(
    page_title="🌌 IntelliPRRSV2 Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ✅ DEBUG BUILD (safe location)
st.write("✅ DEBUG BUILD LOADED:", datetime.now())

# ============================================================
# 🎨 CUSTOM NEON CSS
# ============================================================
st.markdown("""
<style>
body {
    background-color: #0d1117;
    color: #e6edf3;
    font-family: 'Inter', sans-serif;
}
B.block-container {
    padding-top: 1.5rem;
    max-width: 1300px;
    margin: auto;
}
h1 { color: #00f0ff; text-shadow: 0 0 15px #00f0ff; text-align: center; }
h2, h3 { color: #00e6b8; }
.stTabs [data-baseweb="tab-list"] {
    justify-content: center; flex-wrap: wrap; background-color: #0d1117;
    padding: 6px 0; border-bottom: 2px solid #00f0ff30;
}
.stTabs [data-baseweb="tab"] {
    font-weight: 600; color: #fff; background-color: #161b22;
    border-radius: 8px 8px 0 0; margin: 0 5px; padding: 8px 14px;
    box-shadow: 0 0 8px #00f0ff40;
}
.stTabs [aria-selected="true"] {
    background-color: #00f0ff !important; color: #000 !important;
    box-shadow: 0 0 15px #00f0ff;
}
</style>
""", unsafe_allow_html=True)
# ============================================================
# 🧠 HELPER FUNCTIONS — PIPELINE-AWARE LOADER (FINAL FIX)
# ============================================================
@st.cache_data(ttl=300, show_spinner=False)
def load_from_mysql(table_name: str):
    """
    Load data using the latest COMPLETED pipeline run timestamp.
    Single-argument function to avoid cache poisoning.
    """

    with engine.connect() as conn:
        pipeline_ts = conn.execute(
            text("""
                SELECT end_time
                FROM pipeline_runs
                WHERE status = 'complete'
                ORDER BY end_time DESC
                LIMIT 1
            """)
        ).scalar()

    if pipeline_ts is None:
        return pd.DataFrame()

    limit_map = {
        "sequences": 100_000,
        "mutation_hotspots": 100_000,
        "lineage_assignments": 100_000,
        "vaccine_escape_predictions": 50_000,
        "mirna_interactions": 50_000,
        "geo_metadata": 50_000,
        "phylogenetic_tree": 5,
    }

    limit = limit_map.get(table_name, 50_000)

    if table_name == "phylogenetic_tree":
        query = f"""
            SELECT *
            FROM {table_name}
            ORDER BY id DESC
            LIMIT {limit}
        """
        params = None
    else:
        query = f"""
            SELECT *
            FROM {table_name}
            WHERE run_timestamp <= :pipeline_ts
            ORDER BY run_timestamp DESC
            LIMIT {limit}
        """
        params = {"pipeline_ts": pipeline_ts}

    try:
        with engine.connect() as conn:
            return pd.read_sql(text(query), conn, params=params)
    except Exception as e:
        st.error(f"❌ Data load error for {table_name}: {e}")
        return pd.DataFrame()

# ============================================================
# 🕒 TABLE FRESHNESS (READ-ONLY METADATA)
# ============================================================

def get_table_freshness(table_name):
    with engine.connect() as conn:
        return conn.execute(
            text(f"SELECT MAX(run_timestamp) FROM {table_name}")
        ).scalar()


# ============================================================
# 🧩 SAFE PLOTTING WRAPPER (Fixes serialization + width warnings)
# ============================================================
def safe_plotly(fig):
    """Safely render Plotly charts with dtype + period fixes."""
    import pandas as pd

    for trace in fig.data:
        # Normalize Period and Timestamp objects for Plotly
        for attr in ["x", "y"]:
            if hasattr(trace, attr):
                data = getattr(trace, attr)
                if isinstance(data, pd.Series) or isinstance(data, list):
                    data = pd.Series(data)
                    if pd.api.types.is_period_dtype(data):
                        data = data.astype(str)
                    elif pd.api.types.is_datetime64_any_dtype(data):
                        data = data.dt.strftime("%Y-%m-%d")
                    setattr(trace, attr, data.tolist())
    st.plotly_chart(fig, width="stretch")


# ============================================================
# 🧩 SAFE PLOTLY WRAPPER — replaces deprecated use_container_width
# ============================================================
def safe_plotly(fig, width="stretch"):
    """
    Render Plotly charts safely and JSON-compatibly.
    - Converts Period/Timestamp objects automatically
    - Uses new width argument instead of use_container_width
    """
    import plotly.io as pio

    try:
        _ = json.loads(pio.to_json(fig, validate=False))
        st.plotly_chart(fig, width=width)

    except TypeError as e:
        if "Period" in str(e):
            st.warning("Auto-fixing non-serializable Period data → string conversion")
            for trace in fig.data:
                if hasattr(trace, "x"):
                    trace.x = [str(v) for v in trace.x]
            st.plotly_chart(fig, width=width)
        else:
            st.error(f"❌ Plotly rendering error: {e}")
# ============================================================
# ✅ DATABASE VALIDATION
# ============================================================
required_tables = [
    "sequences", "mutation_hotspots", "vaccine_escape_predictions",
    "lineage_assignments", "mirna_interactions",
    "phylogenetic_tree", "geo_metadata"
]
with engine.connect() as conn:
    tables = set(pd.read_sql("SHOW TABLES", conn).iloc[:, 0].tolist())
missing = [t for t in required_tables if t not in tables]
if missing:
    st.warning(f"⚠️ Missing tables: {', '.join(missing)}")
else:
    pass
# ============================================================
# 📡 MYSQL CONNECTION TEST
# ============================================================

def test_mysql_connection():
    """
    Test MySQL connectivity and return latency in milliseconds.
    """
    try:
        start = time.time()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return round((time.time() - start) * 1000, 2)
    except Exception:
        return None

# ============================================================
# ⚡ LATENCY FORMATTER
# ============================================================

def format_latency(ms):
    """
    Format latency for dashboard display.
    """
    if ms is None:
        return "❌ No Connection"
    if ms < 100:
        return f"🟢 {ms} ms"
    if ms < 500:
        return f"🟡 {ms} ms"
    return f"🔴 {ms} ms"


# ============================================================
# 🌌 HEADER METRICS
# ============================================================
st.markdown("<h1>🧬GarbaPRRSV Intelligence Dashboard</h1>",
            unsafe_allow_html=True)
st.markdown("---")

col1, col2, col3, col4 = st.columns(4)
latency = test_mysql_connection()
col3.metric("📡 Status", "🟢 Online" if latency else "🔴 Offline")
col4.metric("⚡ Latency", format_latency(latency))

if latency:
    with engine.connect() as conn:
        seqs = pd.read_sql("SELECT COUNT(*) c FROM sequences", conn).iloc[0, 0]
        preds = pd.read_sql("SELECT COUNT(*) c FROM vaccine_escape_predictions", conn).iloc[0, 0]
    col1.metric("🧬 Sequences", f"{seqs:,}")
    col2.metric("💉 Escape Predictions", f"{preds:,}")

st.markdown("---")
# ============================================================
# 🧭 NAVIGATION
# ============================================================
tabs = st.tabs([
    "🏠 Overview", 
    "🌿 Lineage Trends", 
    "🧬 Mutation Hotspots",
    "💉 Vaccine Escape", 
    "🧠 miRNA Interactions",
    "🌳 Phylogenetic Tree", 
    "🗺️ Geographical Map"
])

# ============================================================
# 🏠 TAB 1 — OVERVIEW
# ============================================================
with tabs[0]:
    st.subheader("📊 PRRSV Genomic Overview")

    df_mut = load_from_mysql("mutation_hotspots")
    df_lin = load_from_mysql("lineage_assignments")
    df_esc = load_from_mysql("vaccine_escape_predictions")

    c1, c2, c3 = st.columns(3)
    c1.metric("🧬 Mutations", len(df_mut))
    c2.metric("🌿 Lineages", df_lin["assigned_lineage"].nunique() if not df_lin.empty else 0)

    if not df_esc.empty:
        df_esc["escape_probability"] = pd.to_numeric(df_esc["escape_probability"], errors="coerce")
        c3.metric("💉 Mean Escape Probability", round(df_esc["escape_probability"].mean(), 3))

    if not df_mut.empty:
        fig_mut = px.line(
            df_mut, x="position", y="mutation_frequency",
            title="Genome-Wide Mutation Frequency", template="plotly_dark"
        )
        safe_plotly(fig_mut)
    else:
        st.info("No mutation data found.")
# ============================================================
# 🌿 TAB 2 — LINEAGE DYNAMICS & EVOLUTION (ISOLATED)
# ============================================================

with tabs[1]:
    st.subheader("🌿 Lineage Dynamics & Evolution")

    df_lin = load_from_mysql("lineage_assignments")

    if df_lin.empty:
        st.warning("⚠️ No lineage data available for the latest pipeline run.")
        st.stop()

    df_lin["run_timestamp"] = pd.to_datetime(df_lin["run_timestamp"], errors="coerce")
    df_lin = df_lin.dropna(subset=["run_timestamp", "assigned_lineage"])

    df_lin["date"] = df_lin["run_timestamp"].dt.date

    latest_ts = df_lin["run_timestamp"].max()
    df_latest = df_lin[df_lin["run_timestamp"] == latest_ts]

    st.caption(f"🕒 Latest snapshot: {latest_ts.strftime('%b %d, %Y — %H:%M:%S')}")

    # --------------------------------------------------------
    # 1️⃣ Emerging Lineage Alerts
    # --------------------------------------------------------
    recent = df_lin[df_lin["run_timestamp"] >= latest_ts - pd.Timedelta(days=7)]
    emerging = recent["assigned_lineage"].value_counts().head(5)

    st.markdown("### 🚨 Emerging Lineages (Last 7 Days)")
    st.dataframe(emerging.reset_index().rename(
        columns={"index": "Lineage", "assigned_lineage": "New Sequences"}
    ))

    # --------------------------------------------------------
    # 2️⃣ Dominance Shift Detection
    # --------------------------------------------------------
    daily = (
        df_lin.groupby(["date", "assigned_lineage"])
        .size()
        .reset_index(name="count")
    )

    top_today = (
        daily[daily["date"] == daily["date"].max()]
        .sort_values("count", ascending=False)
        .head(10)
    )

    fig_dom = px.bar(
        top_today,
        x="assigned_lineage",
        y="count",
        title="Dominant Lineages (Latest Day)",
        template="plotly_dark"
    )
    safe_plotly(fig_dom)

    # --------------------------------------------------------
    # 3️⃣ Growth Rate Analysis
    # --------------------------------------------------------
    growth = (
        daily.groupby("assigned_lineage")["count"]
        .pct_change()
        .reset_index()
        .dropna()
    )

    fig_growth = px.histogram(
        growth,
        x="count",
        nbins=40,
        title="Lineage Growth Rate Distribution",
        template="plotly_dark"
    )
    safe_plotly(fig_growth)

    # --------------------------------------------------------
    # 4️⃣ Growth Heatmap
    # --------------------------------------------------------
    pivot = daily.pivot(
        index="assigned_lineage",
        columns="date",
        values="count"
    ).fillna(0)

    fig_heat = px.imshow(
        pivot.tail(15),
        aspect="auto",
        title="Lineage Activity Heatmap",
        template="plotly_dark"
    )
    safe_plotly(fig_heat)

    # --------------------------------------------------------
    # 5️⃣ Raw Preview
    # --------------------------------------------------------
    with st.expander("🧾 View Raw Lineage Assignments"):
        st.dataframe(
            df_latest[["accession", "assigned_lineage", "run_timestamp"]].head(300),
            use_container_width=True
        )

    st.success(f"✅ Loaded {len(df_lin):,} lineage records")
# ============================================================
# 🧬 TAB 3 — MUTATION HOTSPOTS
# ============================================================

with tabs[2]:

    mut_ts = get_table_freshness("mutation_hotspots")

    if mut_ts:
        st.caption(f"🕒 Last updated: {mut_ts.strftime('%b %d, %Y — %H:%M:%S')}")
    else:
        st.caption("🕒 No timestamp available")

    st.markdown("<h2 style='color:#00f0ff;'>🧬 Mutation Hotspot Intelligence</h2>", unsafe_allow_html=True)

    df_mut = load_from_mysql("mutation_hotspots")

    if df_mut.empty:
        st.warning("⚠️ No mutation hotspot data available.")
    else:

        # Ensure numeric
        df_mut["position"] = pd.to_numeric(df_mut["position"], errors="coerce")
        df_mut["mutation_frequency"] = pd.to_numeric(df_mut["mutation_frequency"], errors="coerce")

        # -----------------------------------------------------
        # 1️⃣ Genome-Wide Mutation Frequency
        # -----------------------------------------------------

        st.markdown("### 📈 Genome-Wide Mutation Frequency")

        fig_line = px.line(
            df_mut,
            x="position",
            y="mutation_frequency",
            template="plotly_dark"
        )

        fig_line.update_layout(
            xaxis_title="Genomic Position (nt)",
            yaxis_title="Mutation Frequency"
        )

        safe_plotly(fig_line)

        # -----------------------------------------------------
        # 2️⃣ ORF-Level Mutation Summary
        # -----------------------------------------------------

        if "orf" in df_mut.columns:

            st.markdown("### 🧬 Mean Mutation Frequency per ORF")

            orf_summary = (
                df_mut.groupby("orf")["mutation_frequency"]
                .mean()
                .reset_index()
                .sort_values("mutation_frequency", ascending=False)
            )

            fig_orf = px.bar(
                orf_summary,
                x="orf",
                y="mutation_frequency",
                template="plotly_dark"
            )

            fig_orf.update_layout(
                xaxis_title="ORF / Gene Region",
                yaxis_title="Mean Mutation Frequency"
            )

            safe_plotly(fig_orf)

        # -----------------------------------------------------
        # 3️⃣ Top 20 Mutation Hotspots
        # -----------------------------------------------------

        st.markdown("### 🔥 Top 20 Hypervariable Positions")

        top_positions = (
            df_mut.groupby("position")["mutation_frequency"]
            .mean()
            .reset_index()
            .sort_values("mutation_frequency", ascending=False)
            .head(20)
        )

        st.dataframe(top_positions.reset_index(drop=True), use_container_width=True)
        
        # Convert to categorical string for proper bar spacing
        top_positions["position_str"] = top_positions["position"].astype(str)

        fig_hotspots = px.bar(
            top_positions,
            x="position_str",
            y="mutation_frequency",
            template="plotly_dark"
        )

        fig_hotspots.update_layout(
            xaxis_title="Genomic Position",
            yaxis_title="Mutation Frequency"
        )

        safe_plotly(fig_hotspots)

        st.success(f"✅ Loaded {len(df_mut):,} mutation records")

# 💉 TAB 4 — VACCINE ESCAPE ============================================================
with tabs[3]:
    esc_ts = get_table_freshness("vaccine_escape_predictions")
    st.caption(f"🕒 Last updated: {esc_ts.strftime('%b %d, %Y — %H:%M:%S')}")

    st.markdown("<h2 style='color:#00f0ff;'>💉 Vaccine Escape Analysis</h2>", unsafe_allow_html=True)

    df_esc = load_from_mysql("vaccine_escape_predictions")

    if not df_esc.empty:
        df_esc["escape_probability"] = pd.to_numeric(df_esc["escape_probability"], errors="coerce")

        st.metric("🌡️ Mean Escape Probability", round(df_esc["escape_probability"].mean(), 3))

        if "predicted_escape_risk" in df_esc.columns:
            pie_data = df_esc["predicted_escape_risk"].value_counts().reset_index()
            pie_data.columns = ["Risk Level", "Count"]

            fig_pie = px.pie(
                pie_data,
                names="Risk Level",
                values="Count",
                hole=0.4,
                template="plotly_dark"
            )
            safe_plotly(fig_pie)

        if "match_score_%" in df_esc.columns:
            fig_match = px.histogram(
                df_esc,
                x="match_score_%",
                nbins=40,
                title="Distribution of Match Scores",
                template="plotly_dark"
            )
            safe_plotly(fig_match)

        if "vaccine" in df_esc.columns:
            mean_escape = (
                df_esc.groupby("vaccine")["escape_probability"]
                .mean()
                .reset_index()
                .sort_values("escape_probability", ascending=False)
            )

            fig_vac = px.bar(
                mean_escape.head(20),
                x="vaccine",
                y="escape_probability",
                title="Mean Escape Probability by Vaccine",
                template="plotly_dark"
            )
            safe_plotly(fig_vac)
    else:
        st.info("No vaccine escape data available.")


# ============================================================
# 🧠 TAB 5 — miRNA INTERACTIONS
# ============================================================

with tabs[4]:
    mir_ts = get_table_freshness("mirna_interactions")
    st.caption(f"🕒 Last updated: {mir_ts.strftime('%b %d, %Y — %H:%M:%S')}")

    st.markdown("<h2 style='color:#00f0ff;'>🧠 miRNA–PRRSV Interaction Intelligence</h2>", unsafe_allow_html=True)

    df_mir = load_from_mysql("mirna_interactions")
    if not df_mir.empty:
        df_mir.columns = [c.lower() for c in df_mir.columns]
        df_mir.rename(columns={"mirna": "miRNA", "target_genome": "target"}, inplace=True)

        st.caption(f"Loaded {len(df_mir):,} interactions from MySQL.")
        st.dataframe(df_mir.head(10), use_container_width=True)

        df_mir["energy_kcal"] = pd.to_numeric(df_mir["energy_kcal"], errors="coerce")

        fig_energy = px.histogram(
            df_mir,
            x="energy_kcal",
            nbins=40,
            title="Binding Energy Distribution",
            template="plotly_dark"
        )
        safe_plotly(fig_energy)

        top_mir = (
            df_mir.groupby("miRNA")["energy_kcal"]
            .mean()
            .reset_index()
            .sort_values("energy_kcal")
            .head(20)
        )

        fig_top = px.bar(
            top_mir,
            x="miRNA",
            y="energy_kcal",
            title="Top miRNAs by Mean Binding Energy",
            template="plotly_dark"
        )
        safe_plotly(fig_top)

        sample = df_mir.sample(min(300, len(df_mir)))
        G = nx.from_pandas_edgelist(sample, source="miRNA", target="target")

        fig_net, ax = plt.subplots(figsize=(7, 6))
        nx.draw_networkx(G, ax=ax, node_size=200, font_size=7, with_labels=False)
        st.pyplot(fig_net)
    else:
        st.info("No miRNA interaction data found.")

# ============================================================
# 🌳 TAB 6 — PHYLOGENETIC TREE VISUALIZATION
# ============================================================

with tabs[5]:
    st.markdown("<h2 style='color:#00f0ff;'>🌳 PRRSV Phylogenetic Tree — Interactive</h2>", unsafe_allow_html=True)

    df_tree = load_from_mysql("phylogenetic_tree")
    if not df_tree.empty and "newick_data" in df_tree.columns:
        try:
            # Latest row is already first (ORDER BY id DESC)
            newick_data = df_tree.iloc[0]["newick_data"]

            handle = StringIO(newick_data)
            tree = Phylo.read(handle, "newick")

            x, y, labels = [], [], []

            def traverse(clade, x_pos=0, y_pos=0, step=1):
                if clade.is_terminal():
                    x.append(x_pos)
                    y.append(y_pos)
                    labels.append(clade.name)
                else:
                    for i, child in enumerate(clade.clades):
                        traverse(child, x_pos + (child.branch_length or 0.01), y_pos - step * i, step)

            traverse(tree.root)

            fig_tree = go.Figure()
            fig_tree.add_trace(go.Scatter(
                x=x, y=y, mode='markers+text',
                text=labels, textposition="middle right",
                marker=dict(size=6, color="#00f0ff"),
                hoverinfo="text"
            ))

            fig_tree.update_layout(
                template="plotly_dark",
                title="🧬 PRRSV Phylogenetic Tree (Latest Run)",
                xaxis_title="Genetic Distance",
                yaxis_title="Hierarchy",
                plot_bgcolor="#0d1117",
                paper_bgcolor="#0d1117",
                xaxis=dict(showgrid=False),
                yaxis=dict(showgrid=False, showticklabels=False)
            )

            safe_plotly(fig_tree)

            st.success("✅ Phylogenetic tree loaded successfully.")

            with st.expander("🧾 View Raw Newick Data"):
                st.code(newick_data[:1200] + "..." if len(newick_data) > 1200 else newick_data)

        except Exception as e:
            st.error(f"⚠️ Error rendering phylogenetic tree: {e}")
    else:
        st.info("No phylogenetic tree data available in MySQL.")
# ============================================================
# 🌍 TAB 7 — GEOGRAPHICAL MAP
# ============================================================

with tabs[6]:
    geo_ts = get_table_freshness("geo_metadata")
    st.caption(f"🕒 Last updated: {geo_ts.strftime('%b %d, %Y — %H:%M:%S')}")

    st.markdown("<h2 style='color:#00f0ff;'>🌍 Global PRRSV Isolate Distribution</h2>", unsafe_allow_html=True)
    
    df_geo = load_from_mysql("geo_metadata") 
    if df_geo.empty:
        st.info("No geographical data available.")
    else:
        df_geo.columns = [c.lower() for c in df_geo.columns]
        df_geo["country"] = df_geo["country"].astype(str).str.strip()
        df_geo["latitude"] = pd.to_numeric(df_geo["latitude"], errors="coerce")
        df_geo["longitude"] = pd.to_numeric(df_geo["longitude"], errors="coerce")

        df_geo = df_geo[df_geo["country"].str.lower() != "unknown"]
        valid_coords = df_geo.dropna(subset=["latitude", "longitude"])

        if not valid_coords.empty:
            fig_geo = px.scatter_geo(
                valid_coords,
                lat="latitude",
                lon="longitude",
                color="country",
                projection="natural earth",
                template="plotly_dark",
                title="PRRSV Isolate Map (Latest Available Data)"
            )
            safe_plotly(fig_geo)
        else:
            country_summary = df_geo["country"].value_counts().reset_index()
            country_summary.columns = ["country", "count"]

            fig_country = px.choropleth(
                country_summary,
                locations="country",
                locationmode="country names",
                color="count",
                template="plotly_dark",
                title="PRRSV Distribution by Country"
            )
            safe_plotly(fig_country)

            st.dataframe(country_summary, use_container_width=True)


# ============================================================
# 🌌 FOOTER — AUTHORITATIVE PIPELINE STATUS (FINAL)
# ============================================================

st.markdown("<hr>", unsafe_allow_html=True)

last_run_ts = "Unavailable"

try:
    with engine.connect() as conn:
        last_run_ts = conn.execute(
            text("""
                SELECT end_time
                FROM pipeline_runs
                WHERE status = 'complete'
                ORDER BY end_time DESC
                LIMIT 1
            """)
        ).scalar()

    if last_run_ts:
        last_run_ts = pd.to_datetime(last_run_ts).strftime("%b %d, %Y — %H:%M:%S")
    else:
        last_run_ts = "Unavailable"

except Exception:
    last_run_ts = "Unavailable"

st.markdown(f"""
<div style='text-align:center; color:#00f0ff;'>
    <b>🧬 GarbaPRRSV Intelligence Dashboard</b><br>
    Pipeline status: ✅ Completed<br>
    Latest pipeline run: {last_run_ts}<br>
    Version: 2.4 · Stable Neon Release<br>
    © {datetime.now().year} GarbaPRRSV Research Group
</div>
""", unsafe_allow_html=True)

