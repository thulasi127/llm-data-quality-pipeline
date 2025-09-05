# streamlit_app/app.py
import streamlit as st
import json, duckdb, pandas as pd, pathlib, datetime as dt

st.set_page_config(page_title="LLM Data Quality", layout="wide")

# ---- minimal styling ----
st.markdown("""
<style>
.block-container {padding-top: 1.2rem; padding-bottom: 1rem;}
.kpi {font-size: 1.8rem; font-weight: 700;}
.small {font-size: 0.85rem; color: #666;}
hr {margin: 0.6rem 0 1rem 0;}
</style>
""", unsafe_allow_html=True)

st.title("LLM Dataset Quality Dashboard")
st.caption("Ingest → Validate → Curate (silver/gold) → Monitor")

# Load run manifests
manif = pathlib.Path(__file__).resolve().parents[1] / "manifests" / "versions.jsonl"
if not manif.exists() or manif.stat().st_size == 0:
    st.info("No runs yet. Produce messages and run the pipeline first.")
    st.stop()

runs = [json.loads(l) for l in manif.read_text().splitlines()]
runs_df = pd.DataFrame(runs)
# tolerate both "run_ts" (YYYYMMDDTHHMMSS) and ISO strings
def parse_ts(ts):
    try:
        return pd.to_datetime(ts, format="%Y%m%dT%H%M%S")
    except Exception:
        return pd.to_datetime(ts, utc=True, errors="coerce")
runs_df["time"] = runs_df["run_ts"].apply(parse_ts)

# Sidebar
st.sidebar.header("Controls")
options = runs_df.sort_values("time")["run_ts"].tolist()
run_label = st.sidebar.selectbox(
    "Select run",
    options=options,
    index=len(options) - 1,
    format_func=lambda r: parse_ts(r).strftime("%Y-%m-%d %H:%M:%S UTC")
)
latest = runs_df[runs_df["run_ts"] == run_label].iloc[0].to_dict()

# KPIs
c1, c2, c3, c4 = st.columns(4)
c1.metric("Ingested", int(latest.get("in", 0)))
c2.metric("Passed", int(latest.get("passed", 0)))
c3.metric("Rejected", int(latest.get("rejected", 0)))
pct = (latest.get("passed", 0) / max(1, latest.get("in", 1))) * 100
c4.metric("Pass %", f"{pct:.1f}%")

st.markdown("<hr/>", unsafe_allow_html=True)

def load_parquet_dir(dir_path: str, limit: int = 5000) -> pd.DataFrame:
    """Load up to `limit` rows from all *.parquet files in a directory."""
    d = pathlib.Path(dir_path)
    if not d.exists():
        return pd.DataFrame()
    files = list(d.glob("*.parquet"))
    if not files:
        return pd.DataFrame()
    con = duckdb.connect(database=":memory:")
    dfs = []
    for f in files:
        df = con.execute("SELECT * FROM read_parquet(?)", [str(f)]).df()
        dfs.append(df)
    if not dfs:
        return pd.DataFrame()
    out = pd.concat(dfs, ignore_index=True)
    return out.head(limit)

# Load current run silver & rejects
silver = load_parquet_dir(latest["silver_path"])
rejects = load_parquet_dir(latest["rejects_path"])

# Left: rejection reasons; Right: domain/category
left, right = st.columns([1, 1])

with left:
    st.subheader("Rejections by reason")
    reasons_dict = latest.get("reasons", {}) or {}
    if reasons_dict:
        reasons_df = pd.DataFrame(list(reasons_dict.items()), columns=["reason", "count"]).set_index("reason")
        st.bar_chart(reasons_df)
    else:
        st.write("No rejections for this run.")

with right:
    st.subheader("Domains & Categories (passed)")
    if not silver.empty:
        cols = []
        if "domain" in silver.columns:
            by_domain = silver["domain"].value_counts().rename_axis("domain").reset_index(name="count")
            cols.append(("By domain", by_domain))
        if "category" in silver.columns:
            by_cat = silver["category"].value_counts().rename_axis("category").reset_index(name="count")
            cols.append(("By category", by_cat))
        if cols:
            c21, c22 = st.columns(2)
            if len(cols) >= 1:
                c21.caption(cols[0][0]); c21.dataframe(cols[0][1], use_container_width=True)
            if len(cols) >= 2:
                c22.caption(cols[1][0]); c22.dataframe(cols[1][1], use_container_width=True)
        else:
            st.write("No domain/category columns present.")
    else:
        st.write("No passed rows in silver for this run.")

st.markdown("<hr/>", unsafe_allow_html=True)

# Trend
st.subheader("Quality Trend")
trend = runs_df.sort_values("time")[["time", "in", "passed"]].copy()
trend["pass_pct"] = (trend["passed"] / trend["in"].clip(lower=1)) * 100
st.line_chart(trend.set_index("time")[["pass_pct"]])

# Tabs
tab1, tab2, tab3 = st.tabs(["Gold sample", "Rejects sample", "Schema & Checks"])

# Gold sample (by date partition path)
gold_dir = pathlib.Path(latest["gold_path"])
gold_files = list(gold_dir.glob("*.parquet"))

with tab1:
    st.caption(f"Gold partition: {gold_dir}")
    if gold_files:
        con = duckdb.connect(database=":memory:")
        gold_df = con.execute("SELECT * FROM read_parquet($1) LIMIT 200", [str(gold_files[0])]).df()
        st.dataframe(gold_df, use_container_width=True)
        if "text_len" in gold_df.columns:
            st.caption("Text length percentiles (gold)")
            pcts = pd.Series(gold_df["text_len"]).quantile([0.1, 0.5, 0.9]).to_frame("length")
            st.table(pcts)
    else:
        st.write("No gold parquet found for the selected run date.")

with tab2:
    if rejects.empty:
        st.write("No rejects in this run.")
    else:
        st.dataframe(rejects.head(200), use_container_width=True)
        st.caption("Showing up to 200 rejected rows with failure_reason")

with tab3:
    st.markdown("""
**Data contract**
- `id` (string, UUID)
- `ts` (ISO-8601 UTC timestamp)
- `text` (string)
- `source` (enum: web, doc, code)
- `domain` (enum: news, code, social, docs)
- `category` (enum: ai, finance, health, education)
- `text_len` (int; derived)

**Validation checks**
- Text length between 20 and 4000
- Language ≈ English (heuristic)
- No profanity
- No duplicates per (source, text) within batch
""")

st.markdown("<hr/>", unsafe_allow_html=True)
st.caption("LLM dataset quality pipeline • streaming ingest • validation • curated versions • live metrics")
