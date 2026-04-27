#!/usr/bin/env python
# coding: utf-8

# Imports
import os
import logging
import sys
import io
import boto3
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, date
from pathlib import Path
from typing import Tuple
import time

from dotenv import load_dotenv

load_dotenv('./secrets.env')

postgres_username = os.environ['ODS_USERNAME']
postgres_password = os.environ['ODS_PASSWORD']
postgres_host = os.environ['ODS_HOST']
postgres_port = os.environ['ODS_PORT']
postgres_database = os.environ['ODS_DATABASE']
postgres_schema = 'bcts_reporting'

# Object storage (S3 Compatible) credentials and configuration
aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
s3_endpoint_url = os.environ["S3_ENDPOINT_URL"]

# S3 bucket name
bucket_name = 'wyprwt'

reports_to_export = [
    "annual_developed_volume",
    "annual_development_ready",
    "licence_issued_advertised_main",
    "licence_issued_with_unbilled_volume_main",
    "licence_sold_to_out_of_province_registrants",
    "licence_transfer",
    "roads_constructed",
    "roads_deactivated",
    "roads_planned_deactivation",
    "roads_transferred_in",
    "roads_transferred_out",
    "silviliability_main",
    "timber_inventory_development_in_progress",
    "timber_inventory_ready_to_develop",
    "timber_inventory_ready_to_sell",
    "volume_advertised_main",
    "weighted_sale_term"
]


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

def fiscal_year_label(d: date) -> str:
    """
    BC Gov fiscal year: Apr 1 -> Mar 31.
    Returns label like 'FY 2024-25'.
    """
    if d.month >= 4:
        start_year = d.year
    else:
        start_year = d.year - 1
    end_year_short = (start_year + 1) 
    return f"Fiscal{end_year_short}"


def quarter_label(end_date):
    """
    Returns fiscal quarter label (Q1–Q4),
    where the fiscal year starts in April.

    Q1: Apr–Jun
    Q2: Jul–Sep
    Q3: Oct–Dec
    Q4: Jan–Mar
    """
    if isinstance(end_date, datetime):
        end_date = end_date.date()

    month = end_date.month

    if 4 <= month <= 6:
        return "Q1"
    elif 7 <= month <= 9:
        return "Q2"
    elif 10 <= month <= 12:
        return "Q3"
    else:  # Jan–Mar
        return "Q4"


def reports_ending_label(d: date) -> str:
    """
    Folder naming like 'Reports Ending 31 August'
    """
    # Day without leading zero + full month name
    return f"End of Reporting Period {d.day} {d.strftime('%B')}"




def extract_report_dates(df: pd.DataFrame, report, default_end_date) -> Tuple[date, date]:
    """
    Pulls report_start_date and report_end_date from the fetched data.
    Assumes both columns exist and are consistent across rows.

    Returns:
        (current_date, end_date) as datetime.date objects.
    """
    report_run_date = date.today()
    if report in ("licence_issued_with_unbilled_volume_main", "silviliability_main"):
        return default_end_date, report_run_date
        
    required_cols = ["report_end_date"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"Missing required column(s): {missing}. Available columns: {list(df.columns)}")

    # Take first non-null values
    end_series   = df["report_end_date"].dropna()

    if end_series.empty:
        raise ValueError("Column 'report_end_date' exists but has no non-null values.")

    end_val   = end_series.iloc[0]

    # Normalize to Python date
    end_ts   = pd.to_datetime(end_val, errors="raise")


    return end_ts.date(), report_run_date


def fetch_from_ods(report_name: str) -> pd.DataFrame:
    logging.info(f"Fetching data from PostgreSQL for: {report_name}")
    try:
        engine = create_engine(
            f"postgresql://{postgres_username}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}"
        )

        select_sql = f"SELECT * FROM {postgres_schema}.{report_name}"
        df = pd.read_sql(text(select_sql), engine)

        logging.info(f"✅ Fetched {len(df):,} rows from {postgres_schema}.{report_name}")
        return df

    except Exception as e:
        logging.error(f"❌ Error fetching from PostgreSQL for {report_name}: {e}")
        sys.exit(1)



def write_to_local(df, report, default_end_date):
    """
    Writes df to ./exports/<report>.xlsx

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame to write
    report : str
        Filename (with or without .xlsx)
    """
    # Ensure exports directory exists
    end_date, report_run_date = extract_report_dates(df, report, default_end_date)
    fy_folder = fiscal_year_label(end_date)
    quarter_folder = quarter_label(end_date)
    ending_folder = reports_ending_label(end_date)
    file_name = f"{report}_{report_run_date}.xlsx"
    print(end_date, fy_folder, quarter_folder, ending_folder)
    
    export_dir = (
        Path(r"F:\!shared_root")
        / "02_DataManagement"
        / "01_PerformanceReports"
        / "TEST_SREE_AUTOMATED_EXPORTS"
        / fy_folder
        / ending_folder
        / "Milestone Reports"
    )

    export_dir.mkdir(parents=True, exist_ok=True)

    filepath = os.path.join(export_dir, file_name)

    with pd.ExcelWriter(
        filepath,
        engine="xlsxwriter",
        engine_kwargs={"options": {"constant_memory": True}}
    ) as writer:
        df.to_excel(writer, index=False, sheet_name=report[:31])
    print(f"File saved to {filepath}")

    return filepath




if __name__ == "__main__":
    loop1 = True
    for report in reports_to_export:
        logging.info(f"--- Processing report: {report} ---")
        df = fetch_from_ods(report)
        if loop1:
            end_series   = df["report_end_date"].dropna()
            end_val   = end_series.iloc[0]
            default_end_date = pd.to_datetime(end_val, errors="raise").date()
            loop1 = False

        # Optional: skip empty datasets (prevents crash on missing report_end_date)
        if df.empty:
            logging.warning(f"⚠️ {report} returned 0 rows; skipping upload.")
            continue

        write_to_local(df, report, default_end_date)

