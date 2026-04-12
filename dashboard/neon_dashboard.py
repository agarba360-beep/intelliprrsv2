#!/usr/bin/env python3
# ============================================================
# 🌌 IntelliPRRSV2 — Neon Bioinformatics Dashboard (Enhanced 2026 Edition)
# ============================================================

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
from io import StringIO
from Bio import Phylo
import numpy as np
from datetime import datetime
from sklearn.linear_model import LinearRegression

# ============================================================
# DATABASE CONFIGURATION
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
# STREAMLIT SETUP
# ============================================================
st.set_page_config(page_title="🌌 IntelliPRRSV2 Dashboard", layout="wide")
st.markdown("<h1 style='text-align:center; color:#00f0ff;'>🧬 IntelliPRRSV2 — Neon Intelligence Dashboard</h1>", unsafe_allow_html=True)
st.markdown("---")

# ============================================================
# CACHING FUNCTIONS
# ============================================================
@st.cache_data(ttl=300)
def load_from_mysql(table):
    try:
        with engine.connect() as conn:
            df = pd.read_sql(f"SELECT * FROM `{table}`", conn)
        return df
    except Exception:
        return pd.DataFrame()

def test_mysql_connection():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False

# ============================================================
# DATABASE STATUS + STATS
# ============================================================
col1, col2, col3 = st.columns(3)
db_online = test_mysql_connection()

total_sequences, total_predictions, last_update = 0, 0, "—"

if db_online:
    with engine.connect() as conn:
        total_sequences = pd.read_sql("SELECT COUNT(*) AS c FROM sequences", conn).iloc[0, 0]
        total_predictions = pd.read_sql("SELECT COUNT(*) AS c FROM vaccine_escape_predictions", conn).iloc[0, 0]
        last_update_row = pd.read_sql("SELECT MAX(run_timestamp) AS last_run FROM sequences", conn)
        last_update = str(last_update_row.iloc[0, 0]) if not last_update_row.empty else "N/A"

col1.metric("🧬 Total Sequences", total_sequences)
col2.metric("💉 Vaccine Predictions", total_predictions)
col3.metric("🕒 Last Pipeline Run", last_update)

st.markdown("---")

# ============================================================
# DATA GROWTH + FORECAST
# ============================================================
st.subheader("📈 Data Growth Over Time (with Forecast)")
df_seq = load_from_mysql("sequences")
if not df_seq.empty and "run_timestamp" in df_seq.columns:
    df_seq["run_timestamp"] = pd.to_datetime(df_seq["run_timestamp"], errors="coerce")
    growth = df_seq.groupby(df_seq["run_timestamp"].dt.date).size().reset_index(name="Sequences")

    if len(growth) > 1:
        growth["day_index"] = np.arange(len(growth))
        model = LinearRegression().fit(growth[["day_index"]], growth["Sequences"])
        future_days = np.arange(len(growth), len(growth) + 5).reshape(-1, 1)
        future_pred = model.predict(future_days)
        future_dates = pd.date_range(growth["run_timestamp"].max(), periods=6, freq="D")[1:]
        forecast_df = pd.DataFrame({"run_timestamp": future_dates, "Sequences (Forecast)": future_pred})

        fig = px.line(growth, x="run_timestamp", y="Sequences", markers=True, title="Pipeline Growth & Forecast", template="plotly_dark")
        fig.add_scatter(x=forecast_df["run_timestamp"], y=forecast_df["Sequences (Forecast)"],
                        mode="lines+markers", line=dict(dash="dot", color="#00ff88"), name="Forecast")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Not enough runs yet for trend forecasting.")
else:
    st.info("No data found in MySQL — run pipeline first.")

# ============================================================
# MAIN NAVIGATION
# ============================================================
tabs = st.tabs(["Overview", "Lineage Trends", "Mutation Hotspots", "Vaccine Escape", "miRNA Interactions", "Phylogenetic Tree", "Geographical Map"])

# ============================================================
# TAB 1 — OVERVIEW
# ============================================================
with tabs[0]:
    st.header("📊 PRRSV Overview")
    df_mut = load_from_mysql("mutation_hotspots")
    df_lin = load_from_mysql("lineage_assignments")

    if not df_lin.empty:
        fig_line = px.pie(df_lin, names="assigned_lineage", title="🌿 Lineage Distribution", hole=0.45, template="plotly_dark")
        st.plotly_chart(fig_line, use_container_width=True)
    if not df_mut.empty:
        fig_mut = px.line(df_mut, x="position", y="mutation_frequency", title="🧬 Mutation Frequency", template="plotly_dark")
        st.plotly_chart(fig_mut, use_container_width=True)

# ============================================================
# TAB 2 — LINEAGE TRENDS (NEW)
# ============================================================
with tabs[1]:
    st.header("🌳 PRRSV Lineage Diversity Over Time")

    df_summary = load_from_mysql("lineage_summary")
    if not df_summary.empty:
        df_summary["run_timestamp"] = pd.to_datetime(df_summary["run_timestamp"], errors="coerce")
        fig_lin = px.line(df_summary, x="run_timestamp", y="count", color="lineage",
                          title="Lineage Dynamics Across Pipeline Runs", template="plotly_dark")
        fig_lin.update_layout(xaxis_title="Run Timestamp", yaxis_title="Isolate Count")
        st.plotly_chart(fig_lin, use_container_width=True)
    else:
        st.info("No lineage summary data found. Run pipeline to populate this table.")

# ============================================================
# TAB 3 — MUTATION HOTSPOTS WITH LINEAGE OVERLAY
# ============================================================
with tabs[2]:
    st.header("🧬 Mutation Hotspots by Lineage")

    df_mut = load_from_mysql("mutation_hotspots")
    df_lin = load_from_mysql("lineage_assignments")
    if not df_mut.empty and not df_lin.empty:
        if "accession" in df_mut.columns:
            merged = pd.merge(df_mut, df_lin, on="accession", how="left")
            fig = px.scatter(merged, x="position", y="mutation_frequency",
                             color="assigned_lineage", title="Mutation Frequency by Lineage",
                             template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Mutation dataset missing accession field.")
    else:
        st.info("Mutation or lineage data missing from MySQL.")

# ============================================================
# TAB 4 — VACCINE ESCAPE
# ============================================================
with tabs[3]:
    st.header("💉 Vaccine Escape Risk Overview")
    df = load_from_mysql("vaccine_escape_predictions")
    if not df.empty:
        fig_pie = px.pie(df, names="predicted_escape_risk", title="Escape Risk Distribution", template="plotly_dark")
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.info("No vaccine escape data found.")

# ============================================================
# TAB 5 — miRNA INTERACTIONS
# ============================================================
with tabs[4]:
    st.header("🧠 miRNA–PRRSV Network")
    df = load_from_mysql("mirna_interactions")
    if not df.empty and "miRNA" in df.columns:
        top = df["miRNA"].value_counts().nlargest(20).reset_index()
        top.columns = ["miRNA", "Count"]
        fig = px.bar(top, x="miRNA", y="Count", color="Count", color_continuous_scale="Plasma", template="plotly_dark")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No miRNA–PRRSV data available yet.")

# ============================================================
# TAB 6 — PHYLOGENETIC TREE
# ============================================================
with tabs[5]:
    st.header("🌿 Biologically Scaled PRRSV Tree")

    try:
        df_tree = pd.read_sql(text("SELECT newick_data FROM phylogenetic_tree ORDER BY id DESC LIMIT 1"), engine)
        if not df_tree.empty:
            newick = df_tree["newick_data"].iloc[0]
            handle = StringIO(newick)
            tree = Phylo.read(handle, "newick")

            from Bio import Phylo
            from io import StringIO
            import plotly.graph_objects as go

            def extract_edges(clade, x=0, y=0, edges=None, coords=None):
                if edges is None: edges, coords = [], {}
                coords[clade] = (x, y)
                for i, sub in enumerate(clade.clades):
                    length = sub.branch_length or 0.001
                    new_x = x + length * 100
                    new_y = y - i * 2
                    edges.append(((x, y), (new_x, new_y)))
                    extract_edges(sub, new_x, new_y, edges, coords)
                return edges, coords

            edges, coords = extract_edges(tree.root)
            edge_x, edge_y = [], []
            for (x0, y0), (x1, y1) in edges:
                edge_x += [x0, x1, None]
                edge_y += [y0, y1, None]

            edge_trace = go.Scatter(x=edge_x, y=edge_y, mode="lines", line=dict(color="#00FFFF", width=2))
            node_x, node_y = zip(*coords.values())
            node_trace = go.Scatter(x=node_x, y=node_y, mode="markers", marker=dict(size=5, color="#00f0ff"))
            fig = go.Figure(data=[edge_trace, node_trace])
            fig.update_layout(template="plotly_dark", title="PRRSV Phylogenetic Tree", showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No tree data found in MySQL.")
    except Exception as e:
        st.error(f"Tree visualization failed: {e}")

# ============================================================
# TAB 7 — GEOGRAPHIC MAP + LINEAGE
# ============================================================
with tabs[6]:
    st.header("🗺️ PRRSV Lineage Distribution by Country")
    df_geo = load_from_mysql("geo_metadata")
    df_lin = load_from_mysql("lineage_assignments")
    if not df_geo.empty and "country" in df_geo.columns:
        df_merged = pd.merge(df_geo, df_lin, on="accession", how="left")
        fig = px.scatter_geo(df_merged, locations="country", locationmode="country names",
                             color="assigned_lineage", hover_name="country",
                             title="Global PRRSV Lineage Spread", template="plotly_dark")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No geo data or lineage assignments available.")

# ============================================================
# FOOTER
# ============================================================
st.markdown("""
<hr style='border:1px solid #222;'>
<div style='text-align:center; color:#777; font-size:0.9rem;'>
🧬 IntelliPRRSV2 © 2026 — Real-time Viral Evolution Intelligence Dashboard
</div>
""", unsafe_allow_html=True)

