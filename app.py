# app.py — NIFTY Shock → Next-Day Bounce Dashboard (Refined, Premium Charts)

import os
import glob
import pandas as pd
import streamlit as st
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window as W
import plotly.express as px

# =========================== Configuration ===========================
BASE = os.getcwd()   # works on Docker, Render, local
OUT_DIR = os.path.join(BASE, "output")
RAW_DIR = os.path.join(BASE, "data")

OVERALL_PARQ = os.path.join(OUT_DIR, "overall_bounce.parquet")
OVERALL_CSV  = os.path.join(OUT_DIR, "overall_bounce.csv")
PER_PARQ     = os.path.join(OUT_DIR, "per_stock_bounce.parquet")
PER_CSV      = os.path.join(OUT_DIR, "per_stock_bounce.csv")

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)

st.set_page_config(page_title="NIFTY Shock-Bounce Analysis", layout="wide")

# =========================== Global Style ===========================
st.markdown(
    """
    <style>
    :root { --bg: #0e1117; --card: #111827; --text: #e6e6e6; --sub: #9ca3af; }
    .main-header {
        background: linear-gradient(90deg, #1f3b73 0%, #2e569f 100%);
        padding: 18px 22px; border-radius: 12px; text-align: center; margin-bottom: 18px;
        box-shadow: 0 8px 26px rgba(0,0,0,0.35);
    }
    .main-header h1 { color: white; margin: 0; font-size: 30px; font-weight: 700; }
    .main-header p  { color: #d7dce3; margin: 6px 0 0; font-size: 15px; }
    div[data-testid="stMetric"] {
        background: var(--card); padding: 18px; border-radius: 12px;
        box-shadow: 0 4px 16px rgba(0,0,0,0.35); text-align: center;
        border: 1px solid rgba(255,255,255,0.06);
    }
    .stDataFrame {
        border-radius: 10px; overflow: hidden; background: var(--card);
        box-shadow: 0 6px 18px rgba(0,0,0,0.35); border: 1px solid rgba(255,255,255,0.06);
    }
    section[data-testid="stSidebar"] {
        background-color: #141a26;
        color: var(--text);
        border-right: 1px solid rgba(255,255,255,0.06);
    }
    section[data-testid="stSidebar"] h1, section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] h3 { color: var(--text); }
    </style>
    """,
    unsafe_allow_html=True
)

st.markdown(
    """
    <div class="main-header">
        <h1>NIFTY Shock-Bounce Analysis</h1>
        <p>Interactive KPIs · Ranked Stocks · Distribution Insights · Drill-down with time tools</p>
    </div>
    """,
    unsafe_allow_html=True
)

# =========================== Plotly Helpers =========================
COLORWAY = ["#6ee7b7","#93c5fd","#fca5a5","#fde68a","#a5b4fc","#67e8f9","#f5a6e6"]
GRID = "rgba(160,160,160,0.15)"
PAPER_BG = "#0e1117"
PLOT_BG  = "#0e1117"
FONT = dict(family="Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial", size=13, color="#E6E6E6")

def style_fig(fig, *, height=380, title_margin=48):
    """Apply a consistent premium style to any Plotly figure."""
    fig.update_layout(
        template="plotly_dark",
        colorway=COLORWAY,
        paper_bgcolor=PAPER_BG,
        plot_bgcolor=PLOT_BG,
        font=FONT,
        margin=dict(l=40, r=20, t=title_margin, b=40),
        hoverlabel=dict(bgcolor="#1f2937", bordercolor="#1f2937", font_size=12, font_family=FONT["family"]),
        height=height,
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            bordercolor="rgba(255,255,255,0.05)",
            orientation="h",
            yanchor="bottom", y=1.02, xanchor="right", x=1
        )
    )
    fig.update_xaxes(showgrid=True, gridcolor=GRID, zeroline=False)
    fig.update_yaxes(showgrid=True, gridcolor=GRID, zeroline=False)
    return fig

def percent_hover(yname="Value (%)"):
    return f"<b>%{{x}}</b><br>{yname}: <b>%{{y:.2f}}</b>%<extra></extra>"

def line_hover(yname):
    return f"<b>%{{x|%Y-%m-%d}}</b><br>{yname}: <b>%{{y:.4f}}</b><extra></extra>"

def add_time_tools(fig):
    fig.update_xaxes(
        rangeselector=dict(
            buttons=list([
                dict(count=1, label="1m", step="month", stepmode="backward"),
                dict(count=3, label="3m", step="month", stepmode="backward"),
                dict(count=6, label="6m", step="month", stepmode="backward"),
                dict(step="all", label="All")
            ])
        ),
        rangeslider=dict(visible=True),
        type="date"
    )

# =========================== Helpers ================================
@st.cache_resource
def get_spark():
    try:
        return (
            SparkSession.builder
            .appName("NiftyStreamlit")
            .master("local[*]")
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

def recompute_from_raw(raw_dir: str, out_dir: str, *, shock_thr=-0.02, bounce_thr=0.0):
    """Rebuild features & KPIs from raw CSVs with proper deduplication and precision."""
    spark = get_spark()
    if spark is None:
        st.error("Spark is required for recompute (JVM unavailable).")
        return

    df = (
        spark.read.csv(f"{raw_dir}/*.csv", header=True, inferSchema=True)
        .withColumn("Date",   F.to_date("Date"))
        .withColumn("Symbol", F.trim(F.col("Symbol")))
        .withColumn("Close",  F.col("Close").cast("double"))
        .dropna(subset=["Date","Symbol","Close"])
    )

    # Deduplicate rows for same (Symbol, Date)
    w_dedup = W.partitionBy("Symbol","Date").orderBy(F.desc("Date"), F.desc("Close"))
    df = df.withColumn("rn", F.row_number().over(w_dedup)).filter("rn = 1").drop("rn")

    w = W.partitionBy("Symbol").orderBy("Date")
    feat = (
        df
        .withColumn("Prev_Close",      F.lag("Close").over(w).cast("double"))
        .withColumn("Daily_Return",    (F.col("Close") - F.col("Prev_Close")) / F.col("Prev_Close"))
        .withColumn("Next_Close",      F.lead("Close").over(w).cast("double"))
        .withColumn("Next_Day_Return", (F.col("Next_Close") - F.col("Close")) / F.col("Close"))
        .withColumn("Shock_Day",   F.when(F.col("Daily_Return") <= shock_thr, 1).otherwise(0))
        .withColumn("Bounce_Back", F.when((F.col("Shock_Day") == 1) & (F.col("Next_Day_Return") > bounce_thr), 1).otherwise(0))
    )

    overall = (
        feat.agg(F.sum("Shock_Day").alias("total_shocks"),
                 F.sum("Bounce_Back").alias("total_bounces"))
            .withColumn("bounce_rate_pct",
                        F.when(F.col("total_shocks") > 0,
                               F.col("total_bounces") * 100.0 / F.col("total_shocks"))
                         .otherwise(F.lit(0.0)))
    )

    per_stock = (
        feat.groupBy("Symbol")
            .agg(F.sum("Shock_Day").alias("shock_count"),
                 F.sum("Bounce_Back").alias("bounce_count"))
            .withColumn("bounce_rate_pct",
                        F.when(F.col("shock_count") > 0,
                               F.col("bounce_count") * 100.0 / F.col("shock_count"))
                         .otherwise(F.lit(0.0)))
            .orderBy(F.col("bounce_rate_pct").desc())
    )

    # Persist results
    overall.coalesce(1).write.mode("overwrite").parquet(os.path.join(out_dir, "overall_bounce.parquet"))
    per_stock.coalesce(1).write.mode("overwrite").parquet(os.path.join(out_dir, "per_stock_bounce.parquet"))
    overall.toPandas().to_csv(os.path.join(out_dir, "overall_bounce.csv"), index=False)
    per_stock.toPandas().to_csv(os.path.join(out_dir, "per_stock_bounce.csv"), index=False)

# =========================== Load Data ==============================
overall  = read_table(OVERALL_PARQ, OVERALL_CSV)
per_stock= read_table(PER_PARQ, PER_CSV)

with st.sidebar:
    st.header("Analysis Parameters")
    drop_threshold   = st.number_input("Shock threshold (%)", value=-2.0, step=0.5)
    bounce_threshold = st.number_input("Next-day bounce (%)", value=0.0,  step=0.5)
    st.caption("These parameters are applied in Drill-down and recompute.")

    if st.button("Recompute KPIs from raw", use_container_width=True):
        with st.spinner("Recomputing from raw CSVs…"):
            recompute_from_raw(
                RAW_DIR, OUT_DIR,
                shock_thr=drop_threshold/100.0,
                bounce_thr=bounce_threshold/100.0
            )
        st.success("KPIs recomputed. Reloading data…")
        overall   = read_table(OVERALL_PARQ, OVERALL_CSV)
        per_stock = read_table(PER_PARQ, PER_CSV)

# Tabs
kpi_tab, dist_tab, drill_tab = st.tabs(["KPIs", "Distributions", "Drill-down"])

# =========================== KPIs ===================================
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
            width="stretch", height=440   # Streamlit's new API
        )
        download_link(per_stock, "per_stock_bounce.csv", "Download full table")

        tab1, tab2 = st.tabs(["Top 10 by bounce rate", "Bottom 10 by bounce rate"])

        with tab1:
            top10 = per_stock.sort_values("bounce_rate_pct", ascending=False).head(10).copy()
            top10["bounce_rate_pct_fmt"] = top10["bounce_rate_pct"].round(2)
            fig = px.bar(
                top10, x="Symbol", y="bounce_rate_pct",
                title="Top 10 Stocks by Bounce Rate",
                text="bounce_rate_pct_fmt", color="bounce_rate_pct",
                color_continuous_scale=["#3fbf8a","#67dca8","#98e9c5","#c9f5e2"]
            )
            fig.update_traces(
                texttemplate="%{text:.2f}%",
                textposition="outside",
                marker_line_color="rgba(255,255,255,0.35)",
                marker_line_width=1
            )
            fig = style_fig(fig, height=380)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": True})

        with tab2:
            bottom10 = per_stock[per_stock["shock_count"] > 0].sort_values("bounce_rate_pct").head(10).copy()
            bottom10["bounce_rate_pct_fmt"] = bottom10["bounce_rate_pct"].round(2)
            fig = px.bar(
                bottom10, x="Symbol", y="bounce_rate_pct",
                title="Bottom 10 Stocks by Bounce Rate",
                text="bounce_rate_pct_fmt", color="bounce_rate_pct",
                color_continuous_scale=["#f59e9e","#f5b8b8","#f8d1d1","#fde6e6"]
            )
            fig.update_traces(
                texttemplate="%{text:.2f}%",
                textposition="outside",
                marker_line_color="rgba(255,255,255,0.35)",
                marker_line_width=1
            )
            fig = style_fig(fig, height=380)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": True})

# =========================== Distributions ===========================
with dist_tab:
    if not per_stock.empty:
        st.subheader("Distribution of Bounce Rates")

        fig_hist = px.histogram(
            per_stock, x="bounce_rate_pct", nbins=24,
            labels={"bounce_rate_pct": "Bounce Rate (%)"},
            title="Histogram of Bounce Rates",
            marginal="box", opacity=0.95
        )
        fig_hist.update_traces(hovertemplate=percent_hover("Bounce Rate (%)"))
        fig_hist = style_fig(fig_hist, height=420)
        st.plotly_chart(fig_hist, use_container_width=True, config={"displayModeBar": True})

        fig_box = px.box(
            per_stock, x="bounce_rate_pct", points=False,
            labels={"bounce_rate_pct": "Bounce Rate (%)"},
            title="Boxplot of Bounce Rates"
        )
        fig_box.update_traces(hovertemplate=percent_hover("Bounce Rate (%)"))
        fig_box = style_fig(fig_box, height=420)
        st.plotly_chart(fig_box, use_container_width=True, config={"displayModeBar": True})

# =========================== Drill-down ==============================
with drill_tab:
    st.subheader("Drill-down by Stock")
    symbols = sorted(per_stock["Symbol"].unique()) if not per_stock.empty else []
    if symbols:
        sel = st.selectbox("Select stock symbol", symbols, index=0)
        st.caption(f"Detailed time-series analysis for {sel}")

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

            # Deduplicate (Symbol, Date)
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
                    fig_close.update_traces(line=dict(width=2.6))
                    add_time_tools(fig_close)
                    fig_close.update_traces(hovertemplate=line_hover("Close"))
                    fig_close = style_fig(fig_close, height=320, title_margin=56)
                    st.plotly_chart(fig_close, use_container_width=True, config={"displayModeBar": True})

                with c2:
                    fig_ret = px.line(pdf, x="Date", y="Daily_Return", title=f"{sel} Daily Returns")
                    fig_ret.update_traces(line=dict(width=2.3))
                    add_time_tools(fig_ret)
                    fig_ret.update_traces(hovertemplate=line_hover("Daily Return"))
                    fig_ret = style_fig(fig_ret, height=320, title_margin=56)
                    st.plotly_chart(fig_ret, use_container_width=True, config={"displayModeBar": True})

                # Shock table
                shocks = pdf[pdf["Shock_Day"] == 1].copy()
                shocks["Daily_Return_%"]     = (shocks["Daily_Return"] * 100).round(2)
                shocks["Next_Day_Return_%"]  = (shocks["Next_Day_Return"] * 100).round(2)

                st.markdown("Recent Shock Days")
                st.dataframe(
                    shocks[["Date","Close","Prev_Close","Daily_Return_%","Next_Close","Next_Day_Return_%","Bounce_Back"]],
                    width="stretch", height=320
                )

                # Equity curve
                pdf = pdf.sort_values("Date").copy()
                pdf["Equity"] = (1.0 + pdf["Daily_Return"].fillna(0)).cumprod()
                fig_eq = px.line(pdf, x="Date", y="Equity", title=f"{sel} Equity Curve")
                fig_eq.update_traces(line=dict(width=2.6))
                add_time_tools(fig_eq)
                fig_eq.update_traces(hovertemplate=line_hover("Equity"))
                fig_eq = style_fig(fig_eq, height=320, title_margin=56)
                st.plotly_chart(fig_eq, use_container_width=True, config={"displayModeBar": True})

                # KPIs for selected stock
                s_total   = int(shocks.shape[0])
                s_bounces = int(shocks["Bounce_Back"].sum())
                s_rate    = (100.0 * s_bounces / s_total) if s_total else 0.0
                k1, k2, k3 = st.columns(3)
                k1.metric(f"{sel} shocks", f"{s_total:,}")
                k2.metric("Bounces", f"{s_bounces:,}")
                k3.metric("Bounce rate (%)", f"{s_rate:.2f}")
                download_link(shocks, f"{sel}_shocks.csv", f"Download {sel} shocks")

st.caption("Built with PySpark + Streamlit • Data paths: ./data and ./output")
