# app.py — NIFTY Shock → Next-Day Bounce Dashboard (portable + Render-ready)

import os
import glob
import pandas as pd
import streamlit as st
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window as W
import plotly.express as px

# --------------------------- Configuration ---------------------------
# Base directory = current working directory (works in Docker + Render)
BASE = os.getcwd()
OUT_DIR = os.path.join(BASE, "output")
RAW_DIR = os.path.join(BASE, "data")

OVERALL_PARQ = os.path.join(OUT_DIR, "overall_bounce.parquet")
OVERALL_CSV  = os.path.join(OUT_DIR, "overall_bounce.csv")
PER_PARQ     = os.path.join(OUT_DIR, "per_stock_bounce.parquet")
PER_CSV      = os.path.join(OUT_DIR, "per_stock_bounce.csv")

st.set_page_config(page_title="NIFTY Shock-Bounce Analysis", layout="wide")

# --------------------------- Helpers (robust) ------------------------
@st.cache_resource
def get_spark():
    """Create SparkSession, but don't crash if JVM not available."""
    try:
        return (
            SparkSession.builder
            .appName("NiftyStreamlit")
            .master("local[*]")  # Force local mode
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
    except Exception as e:
        st.warning(f"⚠️ SparkSession unavailable (fallback to pandas): {e}")
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
                st.warning(f"⚠️ Spark failed to read {parquet_path}, using pandas: {e}")
        try:
            return _pandas_read_parquet_any(parquet_path)
        except Exception as e:
            st.error(f"❌ Failed to read parquet with pandas: {e}")

    if os.path.exists(csv_path):
        try:
            return pd.read_csv(csv_path)
        except Exception as e:
            st.error(f"❌ Failed to read CSV {csv_path}: {e}")

    return pd.DataFrame()
