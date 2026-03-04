"""
manual_upload.py
================
One-off script to manually upload an existing CSV to BigQuery.
Useful for backfilling historical data without running the full scraper.

Usage:
    python manual_upload.py za_pc_parts_master_2026-03-01.csv
    python manual_upload.py za_pc_parts_master_2026-03-01.csv --skip-transform
"""

import sys
import argparse
import pandas as pd
from google.cloud import bigquery

# ==========================================
# Config — update if project changes
# ==========================================
BQ_PROJECT = "dsfsi-486822"
BQ_LOCATION = "europe-west1"
BQ_STAGING_TABLE = f"{BQ_PROJECT}.stg_pc_parts.raw_extracts"
BQ_PROD_TABLE = f"{BQ_PROJECT}.prod_pc_parts.products"

TRANSFORM_SQL = f"""
    MERGE `{BQ_PROD_TABLE}` AS target
    USING (
        SELECT
            SAFE.PARSE_DATE('%Y-%m-%d', Extraction_Date) AS extraction_date,
            Competitor AS competitor,
            Category AS category,
            Title AS title,
            SAFE_CAST(
                REGEXP_REPLACE(
                    REPLACE(REPLACE(Price, 'R', ''), ' ', ''),
                    r'[^0-9.]', ''
                ) AS FLOAT64
            ) AS price,
            URL AS url,
            SKU AS sku,
            Image_URL AS image_url,
            ROW_NUMBER() OVER (
                PARTITION BY Competitor, SKU, Extraction_Date
                ORDER BY Extraction_Date DESC
            ) AS row_num
        FROM `{BQ_STAGING_TABLE}`
        WHERE Extraction_Date = @extraction_date
    ) AS source
    ON target.competitor = source.competitor
        AND target.sku = source.sku
        AND target.extraction_date = source.extraction_date
    WHEN NOT MATCHED AND source.row_num = 1 THEN
        INSERT (extraction_date, competitor, category, title, price, url, sku, image_url)
        VALUES (source.extraction_date, source.competitor, source.category, source.title,
                source.price, source.url, source.sku, source.image_url)
    WHEN MATCHED AND source.row_num = 1 THEN
        UPDATE SET
            category = source.category,
            title = source.title,
            price = source.price,
            url = source.url,
            image_url = source.image_url
"""


def load_csv_to_staging(client, csv_path):
    print(f"\n[1/2] Loading '{csv_path}' into staging...")
    df = pd.read_csv(csv_path)
    print(f"      Rows: {len(df):,} | Columns: {list(df.columns)}")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_dataframe(
        df, BQ_STAGING_TABLE, job_config=job_config, location=BQ_LOCATION
    )
    job.result()
    print(f"      ✓ Loaded {job.output_rows:,} rows into {BQ_STAGING_TABLE}")

    # Return the unique extraction dates so we can transform them all
    return df["Extraction_Date"].dropna().unique().tolist()


def run_transform(client, extraction_dates):
    print(f"\n[2/2] Transforming staging → production for dates: {extraction_dates}")
    for date in extraction_dates:
        print(f"      Processing {date}...")
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("extraction_date", "STRING", date),
            ]
        )
        job = client.query(TRANSFORM_SQL, job_config=job_config, location=BQ_LOCATION)
        job.result()
        print(f"      ✓ {date} merged into {BQ_PROD_TABLE}")


def main():
    parser = argparse.ArgumentParser(description="Manually upload a CSV to BigQuery.")
    parser.add_argument("csv_path", help="Path to the CSV file to upload")
    parser.add_argument(
        "--skip-transform",
        action="store_true",
        help="Only load to staging, skip the production transform",
    )
    args = parser.parse_args()

    client = bigquery.Client(project=BQ_PROJECT)

    extraction_dates = load_csv_to_staging(client, args.csv_path)

    if not args.skip_transform:
        run_transform(client, extraction_dates)

    print("\n✓ Done! Data is ready in BigQuery.")
    print(f"  Staging : {BQ_STAGING_TABLE}")
    print(f"  Production: {BQ_PROD_TABLE}")


if __name__ == "__main__":
    main()
