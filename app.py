# app.py — NIFTY Shock → Next-Day Bounce Dashboard (portable + clean)

import os
import glob
import pandas as pd
import streamlit as st
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window as W
import plotly.express as px

# --------------------------- Configuration ---------------------------
BASE = os.getcwd()  # portable: works on Docker, Render, local
OUT_DIR = os.path.join(BASE, "output")
RAW_DIR = os.path.join(BASE, "data")

OVERALL_PARQ = os.path.join(OUT_DIR, "overall_bounce.parquet")
OVERALL_CSV  = os.path.join(OUT_DIR, "overall_bounce.csv")
PER_PARQ     = os.path.join(OUT_DIR, "per_stock_bounce.parquet")
PER_CSV      = os.path.join(OUT_DIR, "per_stock_bounce.csv")

# Auto-create directories if missing
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)

st.set_page_config(page_title="NIFTY Shock-Bounce Analysis", layout="wide")

# --------------------------- Helpers ------------------------
@st.cache_resource
def get_spark():
    try:
        return (
            SparkSession.builder
            .appName("NiftyStreamlit")
            .master("local[*]")  # force local mode
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
    except Exception as e:
        st.warning(f"SparkSession unavailable (fallback to pandas): {e}")
        return None

def _pandas_read_parquet_any(path: str) -> pd.DataFrame:
    try:
        return pd.read_parquet(path)
    except Exception:
        part_files = glob.glob(os.path.join(path, "part-*.parquet"))
        if part_files:
            return pd.read_parquet(part_files[0])
        raise

def read_table(parquet_path: str, csv_path: str) -> pd.DataFrame:
    if os.path.exists(parquet_path):
        sp = get_spark()
        if sp is not None:
            try:
                return sp.read.parquet(parquet_path).toPandas()
            except Exception as e:
                st.warning(f"Spark failed to read {parquet_path}, using pandas: {e}")
        try:
            return _pandas_read_parquet_any(parquet_path)
        except Exception as e:
            st.error(f"Failed to read parquet with pandas: {e}")

    if os.path.exists(csv_path):
        try:
            return pd.read_csv(csv_path)
        except Exception as e:
            st.error(f"Failed to read CSV {csv_path}: {e}")

    return pd.DataFrame()

def download_link(df: pd.DataFrame, filename: str, label: str):
    if df is not None and not df.empty:
        st.download_button(label, df.to_csv(index=False).encode("utf-8"),
                           file_name=filename, mime="text/csv")

def recompute_from_raw(raw_dir: str, out_dir: str, shock_thr=-0.02, bounce_thr=0.0):
    spark = get_spark()
    if spark is None:
        st.error("Spark is required for recompute (JVM unavailable). Restart container and try again.")
        return

    df = (
        spark.read.csv(f"{raw_dir}/*.csv", header=True, inferSchema=True)
        .withColumn("Date", F.to_date("Date"))
        .withColumn("Symbol", F.trim(F.col("Symbol")))
        .withColumn("Close", F.col("Close").cast("double"))
        .dropna(subset=["Date", "Symbol", "Close"])
    )

    # Deduplicate rows for same (Symbol, Date)
    w_dedup = W.partitionBy("Symbol", "Date").orderBy(F.desc("Date"), F.desc("Close"))
    df = df.withColumn("rn", F.row_number().over(w_dedup)).filter("rn = 1").drop("rn")

    w = W.partitionBy("Symbol").orderBy("Date")

    feat = (
        df
        .withColumn("Prev_Close", F.lag("Close").over(w).cast("double"))
        .withColumn("Daily_Return",
                    (F.col("Close") - F.col("Prev_Close")) / F.col("Prev_Close"))
        .withColumn("Next_Close", F.lead("Close").over(w).cast("double"))
        .withColumn("Next_Day_Return",
                    (F.col("Next_Close") - F.col("Close")) / F.col("Close"))
        .withColumn("Shock_Day",
                    F.when(F.col("Daily_Return") <= shock_thr, 1).otherwise(0))
        .withColumn("Bounce_Back",
                    F.when((F.col("Shock_Day") == 1) &
                           (F.col("Next_Day_Return") > bounce_thr), 1).otherwise(0))
    )

    # Overall KPIs
    overall = (
        feat.agg(F.sum("Shock_Day").alias("total_shocks"),
                 F.sum("Bounce_Back").alias("total_bounces"))
            .withColumn(
                "bounce_rate_pct",
                F.when(F.col("total_shocks") > 0,
                       F.col("total_bounces") * 100.0 / F.col("total_shocks"))
                 .otherwise(F.lit(0.0))
            )
    )

    # Per-stock KPIs
    per_stock = (
        feat.groupBy("Symbol")
            .agg(F.sum("Shock_Day").alias("shock_count"),
                 F.sum("Bounce_Back").alias("bounce_count"))
            .withColumn(
                "bounce_rate_pct",
                F.when(F.col("shock_count") > 0,
                       F.col("bounce_count") * 100.0 / F.col("shock_count"))
                 .otherwise(F.lit(0.0))
            )
            .orderBy(F.col("bounce_rate_pct").desc())
    )

    # Persist results
    overall.coalesce(1).write.mode("overwrite").parquet(f"{out_dir}/overall_bounce.parquet")
    per_stock.coalesce(1).write.mode("overwrite").parquet(f"{out_dir}/per_stock_bounce.parquet")
    overall.toPandas().to_csv(f"{out_dir}/overall_bounce.csv", index=False)
    per_stock.toPandas().to_csv(f"{out_dir}/per_stock_bounce.csv", index=False)

# --------------------------- UI / Load ---------------------------
st.title("NIFTY Shock-Bounce Analysis Dashboard")

overall = read_table(OVERALL_PARQ, OVERALL_CSV)
per_stock = read_table(PER_PARQ, PER_CSV)

with st.sidebar:
    st.header("Analysis Parameters")
    drop_threshold = st.number_input("Shock threshold (%)", value=-2.0, step=0.5)
    bounce_threshold = st.number_input("Next-day bounce (%)", value=0.0, step=0.5)
    st.caption("Applied to the Drill-down and to recompute.")

    if st.button("Recompute KPIs from raw"):
        with st.spinner("Recomputing from raw CSVs…"):
            recompute_from_raw(
                RAW_DIR, OUT_DIR,
                shock_thr=drop_threshold / 100.0,
                bounce_thr=bounce_threshold / 100.0
            )
        st.success("KPIs recomputed and saved. Reloading…")
        overall = read_table(OVERALL_PARQ, OVERALL_CSV)
        per_stock = read_table(PER_PARQ, PER_CSV)

# --------------------------- KPI Cards ---------------------------
if not overall.empty:
    s_col = "total_shocks" if "total_shocks" in overall.columns else ("num_shocks" if "num_shocks" in overall.columns else None)
    b_col = "total_bounces" if "total_bounces" in overall.columns else ("num_bounces" if "num_bounces" in overall.columns else None)
    r_col = "bounce_rate_pct" if "bounce_rate_pct" in overall.columns else ("bounce_rate_percent" if "bounce_rate_percent" in overall.columns else None)

    if s_col and b_col and r_col:
        total_shocks  = int(overall[s_col].iloc[0])
        total_bounces = int(overall[b_col].iloc[0])
        bounce_rate   = float(overall[r_col].iloc[0])

        c1, c2, c3 = st.columns(3)
        c1.metric("Total shocks", f"{total_shocks:,}")
        c2.metric("Total bounces", f"{total_bounces:,}")
        c3.metric("Bounce rate (%)", f"{bounce_rate:.2f}")

# --------------------------- Per-Stock Section --------------------
if not per_stock.empty:
    st.subheader("Per-Stock Bounce Metrics")
    per_stock = per_stock.rename(columns=str.strip)

    st.dataframe(
        per_stock.sort_values("bounce_rate_pct", ascending=False),
        width="stretch", height=420
    )
    download_link(per_stock, "per_stock_bounce.csv", "Download full table")

    tab1, tab2 = st.tabs(["Top 10 by bounce rate", "Bottom 10 by bounce rate"])
    with tab1:
        top10 = per_stock.sort_values("bounce_rate_pct", ascending=False).head(10)
        st.bar_chart(top10.set_index("Symbol")["bounce_rate_pct"], height=360)
    with tab2:
        bottom10 = per_stock[per_stock["shock_count"] > 0].sort_values("bounce_rate_pct").head(10)
        st.bar_chart(bottom10.set_index("Symbol")["bounce_rate_pct"], height=360)

# --------------------------- Distribution Visuals --------------------
if not per_stock.empty:
    st.subheader("Distribution of Bounce Rates")

    fig_hist = px.histogram(
        per_stock,
        x="bounce_rate_pct",
        nbins=20,
        labels={"bounce_rate_pct": "Bounce Rate (%)"},
        title="Histogram of Bounce Rates"
    )
    st.plotly_chart(fig_hist, width="stretch")

    fig_box = px.box(
        per_stock,
        x="bounce_rate_pct",
        points=False,
        labels={"bounce_rate_pct": "Bounce Rate (%)"},
        title="Boxplot of Bounce Rates"
    )
    st.plotly_chart(fig_box, width="stretch")

# --------------------------- Drill-down ---------------------------
st.subheader("Drill-down by stock")

symbols = sorted(per_stock["Symbol"].unique()) if not per_stock.empty else []
if symbols:
    sel = st.selectbox("Select symbol", symbols, index=0)

    spark = get_spark()
    if spark is None:
        st.warning("Spark not available; drill-down requires Spark. Restart container and retry.")
    else:
        df = (
            spark.read.csv(f"{RAW_DIR}/*.csv", header=True, inferSchema=True)
            .withColumn("Date", F.to_date("Date"))
            .withColumn("Symbol", F.trim(F.col("Symbol")))
            .withColumn("Close", F.col("Close").cast("double"))
            .dropna(subset=["Date", "Symbol", "Close"])
            .filter(F.col("Symbol") == sel)
        )

        w_ded = W.partitionBy("Symbol", "Date").orderBy(F.desc("Date"), F.desc("Close"))
        df = df.withColumn("rn", F.row_number().over(w_ded)).filter("rn = 1").drop("rn")

        w = W.partitionBy("Symbol").orderBy("Date")
        feat = (
            df
            .withColumn("Prev_Close", F.lag("Close").over(w).cast("double"))
            .withColumn("Daily_Return", (F.col("Close") - F.col("Prev_Close")) / F.col("Prev_Close"))
            .withColumn("Next_Close", F.lead("Close").over(w).cast("double"))
            .withColumn("Next_Day_Return", (F.col("Next_Close") - F.col("Close")) / F.col("Close"))
            .withColumn("Shock_Day", F.when(F.col("Daily_Return") <= (drop_threshold / 100.0), 1).otherwise(0))
            .withColumn("Bounce_Back", F.when((F.col("Shock_Day") == 1) & (F.col("Next_Day_Return") > (bounce_threshold / 100.0)), 1).otherwise(0))
            .orderBy("Date")
        )

        pdf = feat.select(
            "Date", "Close", "Prev_Close", "Daily_Return",
            "Next_Close", "Next_Day_Return", "Shock_Day", "Bounce_Back"
        ).toPandas()

        if not pdf.empty:
            c1, c2 = st.columns(2)
            with c1:
                st.caption("Close (₹)")
                st.line_chart(pdf.set_index("Date")["Close"], height=280)
            with c2:
                st.caption("Daily return (fraction)")
                st.line_chart(pdf.set_index("Date")["Daily_Return"], height=280)

            shocks = pdf[pdf["Shock_Day"] == 1].copy()
            shocks["Daily_Return_%"] = (shocks["Daily_Return"] * 100).round(2)
            shocks["Next_Day_Return_%"] = (shocks["Next_Day_Return"] * 100).round(2)

            st.markdown("Recent shock days")
            st.dataframe(
                shocks[["Date", "Close", "Prev_Close", "Daily_Return_%", "Next_Close", "Next_Day_Return_%", "Bounce_Back"]],
                width="stretch", height=320
            )

            pdf = pdf.sort_values("Date").copy()
            pdf["Equity"] = (1.0 + pdf["Daily_Return"].fillna(0)).cumprod()

            st.caption("Equity Curve (Cumulative Return; starts at 1.0)")
            st.line_chart(pdf.set_index("Date")["Equity"], height=280)

            s_total = int(shocks.shape[0])
            s_bounces = int(shocks["Bounce_Back"].sum())
            s_rate = (100.0 * s_bounces / s_total) if s_total else 0.0
            k1, k2, k3 = st.columns(3)
            k1.metric(f"{sel} shocks", f"{s_total:,}")
            k2.metric("Bounces", f"{s_bounces:,}")
            k3.metric("Bounce rate (%)", f"{s_rate:.2f}")

            download_link(shocks, f"{sel}_shocks.csv", f"Download {sel} shocks")

st.caption("Built with PySpark + Streamlit • Data paths: ./data and ./output")
