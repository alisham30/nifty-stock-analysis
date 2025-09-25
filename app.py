# app.py — NIFTY Shock → Next-Day Bounce Dashboard (Clean + Modern UI)

import os
import glob
import pandas as pd
import streamlit as st
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window as W
import plotly.express as px

# --------------------------- Configuration ---------------------------
BASE = os.getcwd()  # works on Docker, Render, local
OUT_DIR = os.path.join(BASE, "output")
RAW_DIR = os.path.join(BASE, "data")

OVERALL_PARQ = os.path.join(OUT_DIR, "overall_bounce.parquet")
OVERALL_CSV  = os.path.join(OUT_DIR, "overall_bounce.csv")
PER_PARQ     = os.path.join(OUT_DIR, "per_stock_bounce.parquet")
PER_CSV      = os.path.join(OUT_DIR, "per_stock_bounce.csv")

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)

st.set_page_config(page_title="NIFTY Shock-Bounce Analysis", layout="wide")

# --------------------------- Custom Styling ---------------------------
st.markdown(
    """
    <style>
    .main-header {
        background: linear-gradient(to right, #1e3c72, #2a5298);
        padding: 18px;
        border-radius: 10px;
        text-align: center;
        margin-bottom: 20px;
    }
    .main-header h1 {
        color: white;
        margin: 0;
        font-size: 30px;
    }
    .main-header p {
        color: #f0f0f0;
        margin: 0;
        font-size: 16px;
    }
    div[data-testid="stMetric"] {
        background-color: #0e1117;
        padding: 18px;
        border-radius: 12px;
        box-shadow: 0px 3px 10px rgba(0,0,0,0.3);
        text-align: center;
    }
    section[data-testid="stSidebar"] {
        background-color: #1e2130;
        color: white;
    }
    section[data-testid="stSidebar"] h1,
    section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] h3 {
        color: #f0f0f0;
    }
    .stDataFrame {
        border-radius: 8px;
        overflow: hidden;
        background: #0e1117;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# --------------------------- Header ---------------------------
st.markdown(
    """
    <div class="main-header">
        <h1>NIFTY Shock-Bounce Analysis</h1>
        <p>Interactive KPIs • Stock Drilldowns • Distribution Insights</p>
    </div>
    """,
    unsafe_allow_html=True
)

# --------------------------- Helpers ---------------------------
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
        st.warning(f"SparkSession unavailable, falling back to pandas: {e}")
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

# --------------------------- Load Data ---------------------------
overall = read_table(OVERALL_PARQ, OVERALL_CSV)
per_stock = read_table(PER_PARQ, PER_CSV)

with st.sidebar:
    st.header("Analysis Parameters")
    drop_threshold = st.number_input("Shock threshold (%)", value=-2.0, step=0.5)
    bounce_threshold = st.number_input("Next-day bounce (%)", value=0.0, step=0.5)
    st.caption("These parameters are applied in Drill-down and recompute.")

# Tabs for navigation
kpi_tab, dist_tab, drill_tab = st.tabs(["KPIs", "Distributions", "Drill-down"])

# --------------------------- KPI Tab ---------------------------
with kpi_tab:
    if not overall.empty:
        s_col = "total_shocks" if "total_shocks" in overall.columns else None
        b_col = "total_bounces" if "total_bounces" in overall.columns else None
        r_col = "bounce_rate_pct" if "bounce_rate_pct" in overall.columns else None

        if s_col and b_col and r_col:
            total_shocks  = int(overall[s_col].iloc[0])
            total_bounces = int(overall[b_col].iloc[0])
            bounce_rate   = float(overall[r_col].iloc[0])

            c1, c2, c3 = st.columns(3)
            c1.metric("Total shocks", f"{total_shocks:,}")
            c2.metric("Total bounces", f"{total_bounces:,}")
            c3.metric("Bounce rate (%)", f"{bounce_rate:.2f}")

    if not per_stock.empty:
        st.subheader("Per-Stock Bounce Metrics")
        per_stock = per_stock.rename(columns=str.strip)

        st.dataframe(
            per_stock.sort_values("bounce_rate_pct", ascending=False),
            use_container_width=True, height=420
        )
        download_link(per_stock, "per_stock_bounce.csv", "Download full table")

        tab1, tab2 = st.tabs(["Top 10 by bounce rate", "Bottom 10 by bounce rate"])
        with tab1:
            top10 = per_stock.sort_values("bounce_rate_pct", ascending=False).head(10)
            fig = px.bar(
                top10, x="Symbol", y="bounce_rate_pct",
                title="Top 10 Stocks by Bounce Rate", text="bounce_rate_pct",
                color="bounce_rate_pct", color_continuous_scale="Viridis"
            )
            fig.update_traces(texttemplate='%{text:.2f}%', textposition='outside')
            fig.update_layout(height=360)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": True})

        with tab2:
            bottom10 = per_stock[per_stock["shock_count"] > 0].sort_values("bounce_rate_pct").head(10)
            fig = px.bar(
                bottom10, x="Symbol", y="bounce_rate_pct",
                title="Bottom 10 Stocks by Bounce Rate", text="bounce_rate_pct",
                color="bounce_rate_pct", color_continuous_scale="Plasma"
            )
            fig.update_traces(texttemplate='%{text:.2f}%', textposition='outside')
            fig.update_layout(height=360)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": True})

# --------------------------- Distribution Tab ---------------------------
with dist_tab:
    if not per_stock.empty:
        st.subheader("Distribution of Bounce Rates")

        fig_hist = px.histogram(
            per_stock, x="bounce_rate_pct",
            nbins=20, labels={"bounce_rate_pct": "Bounce Rate (%)"},
            title="Histogram of Bounce Rates"
        )
        fig_hist.update_layout(height=400)
        st.plotly_chart(fig_hist, use_container_width=True, config={"displayModeBar": True})

        fig_box = px.box(
            per_stock, x="bounce_rate_pct", points=False,
            labels={"bounce_rate_pct": "Bounce Rate (%)"},
            title="Boxplot of Bounce Rates"
        )
        fig_box.update_layout(height=400)
        st.plotly_chart(fig_box, use_container_width=True, config={"displayModeBar": True})

# --------------------------- Drill-down Tab ---------------------------
with drill_tab:
    st.subheader("Drill-down by Stock")
    symbols = sorted(per_stock["Symbol"].unique()) if not per_stock.empty else []
    if symbols:
        sel = st.selectbox("Select stock symbol", symbols, index=0)
        st.info(f"Showing detailed analysis for {sel}")

        spark = get_spark()
        if spark is None:
            st.warning("Spark not available; drill-down requires Spark.")
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
                    fig_close = px.line(pdf, x="Date", y="Close", title=f"{sel} Closing Price")
                    fig_close.update_layout(height=280)
                    st.plotly_chart(fig_close, use_container_width=True, config={"displayModeBar": True})
                with c2:
                    fig_ret = px.line(pdf, x="Date", y="Daily_Return", title=f"{sel} Daily Returns")
                    fig_ret.update_layout(height=280)
                    st.plotly_chart(fig_ret, use_container_width=True, config={"displayModeBar": True})

                shocks = pdf[pdf["Shock_Day"] == 1].copy()
                shocks["Daily_Return_%"] = (shocks["Daily_Return"] * 100).round(2)
                shocks["Next_Day_Return_%"] = (shocks["Next_Day_Return"] * 100).round(2)

                st.markdown("Recent Shock Days")
                st.dataframe(
                    shocks[["Date", "Close", "Prev_Close", "Daily_Return_%", "Next_Close", "Next_Day_Return_%", "Bounce_Back"]],
                    use_container_width=True, height=320
                )

                pdf = pdf.sort_values("Date").copy()
                pdf["Equity"] = (1.0 + pdf["Daily_Return"].fillna(0)).cumprod()
                fig_eq = px.line(pdf, x="Date", y="Equity", title=f"{sel} Equity Curve")
                fig_eq.update_layout(height=280)
                st.plotly_chart(fig_eq, use_container_width=True, config={"displayModeBar": True})

                s_total = int(shocks.shape[0])
                s_bounces = int(shocks["Bounce_Back"].sum())
                s_rate = (100.0 * s_bounces / s_total) if s_total else 0.0
                k1, k2, k3 = st.columns(3)
                k1.metric(f"{sel} shocks", f"{s_total:,}")
                k2.metric("Bounces", f"{s_bounces:,}")
                k3.metric("Bounce rate (%)", f"{s_rate:.2f}")

                download_link(shocks, f"{sel}_shocks.csv", f"Download {sel} shocks")

st.caption("Built with PySpark + Streamlit • Data paths: ./data and ./output")
