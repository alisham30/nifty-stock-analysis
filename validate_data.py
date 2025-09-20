"""
validate_data.py — simple, dependency-free checks.
Writes /home/jovyan/work/output/data_quality_report.json
"""

import os, glob, json
import pandas as pd
from datetime import datetime

DATA_DIR = "/home/jovyan/work/data"
OUT      = "/home/jovyan/work/output"

def run_checks():
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    report = {
        "run_ts": datetime.utcnow().isoformat() + "Z",
        "num_files": len(files),
        "file_examples": files[:5],
        "checks": []
    }

    if not files:
        report["checks"].append({"name":"files_exist", "status":"FAIL", "details":"No CSV files found"})
        with open(os.path.join(OUT,"data_quality_report.json"), "w") as f:
            json.dump(report, f, indent=2)
        print(" No CSVs found.")
        return

    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)

    # Required columns check
    required = {"Date","Symbol","Prev Close","Close"}
    missing = list(required - set(df.columns))
    report["checks"].append({"name":"required_columns",
                             "status":"PASS" if not missing else "FAIL",
                             "missing":missing})

    # Numeric / non-negative prices
    df["Prev Close"] = pd.to_numeric(df["Prev Close"], errors="coerce")
    df["Close"]      = pd.to_numeric(df["Close"], errors="coerce")
    neg_prev = int((df["Prev Close"] < 0).sum())
    neg_close= int((df["Close"] < 0).sum())
    report["checks"].append({"name":"non_negative_prices",
                             "status":"PASS" if (neg_prev+neg_close)==0 else "WARN",
                             "negative_prev_close":neg_prev,
                             "negative_close":neg_close})

    # Parseable dates
    ok_dates = pd.to_datetime(df["Date"], errors="coerce").notna().mean()
    report["checks"].append({"name":"parseable_dates",
                             "status":"PASS" if ok_dates>0.99 else "WARN",
                             "ratio_parseable": round(ok_dates,4)})

    # Duplicate rows
    dups = int(df.duplicated(subset=["Date","Symbol"]).sum())
    report["checks"].append({"name":"duplicate_rows",
                             "status":"PASS" if dups==0 else "WARN",
                             "duplicates": dups})

    os.makedirs(OUT, exist_ok=True)
    with open(os.path.join(OUT,"data_quality_report.json"), "w") as f:
        json.dump(report, f, indent=2)
    print("Wrote data_quality_report.json\n", json.dumps(report, indent=2))

if __name__ == "__main__":
    run_checks()
