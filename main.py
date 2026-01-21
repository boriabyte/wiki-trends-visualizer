#***********************************************************************************
#
# trend visualization on wikipedia:
# this application will focus on utilizing certain already-made APIs
# that act as data aggregators; big data processing will be done in this application
# together with the visualization and dashboard-like design for user accesibility
#
# used packages:
#           - request, pandas, polars, pyarrow, duckdb - for data fetching
#           - streamlit, plotly - for dashboard
#           - tqdm, python-dateutil, humanize - for visualization
#***********************************************************************************

from request_data import fetch_data
from process_data import process_data
from aggregate_data import aggregate_data
from create_features import build_features
from build_trending import build_trends

import subprocess
import sys
import os

def main():
    print("=== Wikipedia Trend Visualizer ===")
    print("Fetching raw data from https://dumps.wikimedia.org...")
    fetch_data()

    print("All present data was fetched. Processing...")
    process_data()

    print("Data has been processed. Aggregating...")
    aggregate_data()

    print("Aggregation done. Building features...")
    build_features()

    print("Features built. Forming trends...")
    build_trends()

    print("Trends built. Launching dashboard...")

    subprocess.run(
        [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            "dashboard.py",
        ],
        check=True,
    )

if __name__ == "__main__":
    main()
    
