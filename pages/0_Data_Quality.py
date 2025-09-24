import os, json, sys, subprocess
import pandas as pd
import streamlit as st
import validate_data  # direct import instead of subprocess

# Use current working directory
BASE_DIR = os.getcwd()
OUT_DIR  = os.path.join(BASE_DIR, "output")
REPORT   = os.path.join(OUT_DIR, "data_quality_report.json")

st.set_page_config(page_title="Data Quality", page_icon="🧪", layout="wide")
st.title("Data Quality Report")

colA, colB = st.columns([1,4])
with colA:
    if st.button("Run validation now"):
        # Call the function directly, not subprocess
        report = validate_data.run_checks()
        st.success("Validation finished.")
        st.json(report)
        st.rerun()

with colB:
    st.caption("Reads output/data_quality_report.json and summarizes checks.")

# Load report if it exists
if os.path.exists(REPORT):
    with open(REPORT, "r") as f:
        data = json.load(f)

    top = st.container()
    with top:
        st.subheader("Summary")
        c1, c2 = st.columns(2)
        c1.metric("Files scanned", data.get("num_files", 0))
        c2.caption(f"Run at: {data.get('run_ts', 'n/a')}")

    st.divider()

    # Checks table
    checks_df = pd.DataFrame(data.get("checks", []))
    st.subheader("Checks")
    st.dataframe(checks_df, use_container_width=True)

    # Example files
    with st.expander("File examples"):
        st.write("\n".join(data.get("file_examples", [])))

else:
    st.warning("No report found. Click **Run validation now** to generate one.")
