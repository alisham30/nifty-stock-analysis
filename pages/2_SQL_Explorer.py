# pages/1_SQL_Explorer.py
import streamlit as st
import duckdb

st.set_page_config(page_title="SQL Explorer", layout="wide")
st.title("SQL Explorer (DuckDB on Parquet)")

st.caption("Type any SQL query below. Available tables: per_stock, overall.")

# File paths
PER_STOCK = "/home/jovyan/work/output/per_stock.parquet"
OVERALL   = "/home/jovyan/work/output/overall.parquet"

# DuckDB in-memory connection
con = duckdb.connect(database=":memory:")
con.execute(f"CREATE OR REPLACE VIEW per_stock AS SELECT * FROM read_parquet('{PER_STOCK}')")
con.execute(f"CREATE OR REPLACE VIEW overall   AS SELECT * FROM read_parquet('{OVERALL}')")

# Input area for SQL
default_query = "SELECT * FROM per_stock LIMIT 10;"
sql = st.text_area("Enter SQL query:", default_query, height=180)

# Run query
if st.button("Run query"):
    try:
        result = con.execute(sql).df()
        st.success(f"Query executed successfully. Returned {len(result)} rows.")
        st.dataframe(result, use_container_width=True, height=500)

        # Download option
        csv = result.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Download results as CSV",
            data=csv,
            file_name="query_results.csv",
            mime="text/csv",
            use_container_width=True
        )
    except Exception as e:
        st.error(f"SQL Error: {e}")
