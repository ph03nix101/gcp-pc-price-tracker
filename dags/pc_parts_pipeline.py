"""
PC Parts Price Tracker — Airflow DAG
=====================================
Orchestrates the data flow from GCS (raw CSV) → BigQuery Staging → BigQuery Production.

Schedule: Runs every Sunday at 2:00 AM SAST (after the Saturday 11 PM scraper completes).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator


# ==========================================
# Configuration
# ==========================================
PROJECT_ID = "dsfsi-486822"
GCS_BUCKET = "za-pc-parts-raw"
BQ_STAGING_DATASET = "stg_pc_parts"
BQ_STAGING_TABLE = "raw_extracts"
BQ_PROD_DATASET = "prod_pc_parts"
BQ_PROD_TABLE = "products"

# Dynamic date-based GCS path (matches main.py's upload pattern)
# e.g. inbound/2026-03-08/za_pc_parts_master_2026-03-08.csv
EXECUTION_DATE = "{{ ds }}"  # Airflow template: YYYY-MM-DD
GCS_OBJECT_PATH = f"inbound/{EXECUTION_DATE}/za_pc_parts_master_{EXECUTION_DATE}.csv"

# ==========================================
# DAG Default Arguments
# ==========================================
default_args = {
    "owner": "pc-parts-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ==========================================
# DAG Definition
# ==========================================
with DAG(
    dag_id="pc_parts_gcs_to_bigquery",
    default_args=default_args,
    description="Load scraped PC parts data from GCS into BigQuery staging, transform, and serve to production.",
    schedule_interval="0 2 * * 0",  # Every Sunday at 2:00 AM SAST
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=["pc-parts", "scraper", "bigquery"],
) as dag:

    # ------------------------------------------
    # Task 1: Wait for the CSV to appear in GCS
    # ------------------------------------------
    wait_for_csv = GCSObjectExistenceSensor(
        task_id="wait_for_csv_in_gcs",
        bucket=GCS_BUCKET,
        object=GCS_OBJECT_PATH,
        google_cloud_conn_id="google_cloud_default",
        timeout=3600,           # Wait up to 1 hour
        poke_interval=300,      # Check every 5 minutes
        mode="poke",
    )

    # ------------------------------------------
    # Task 2: Load CSV into BigQuery Staging
    # ------------------------------------------
    load_to_staging = GCSToBigQueryOperator(
        task_id="load_csv_to_bq_staging",
        bucket=GCS_BUCKET,
        source_objects=[GCS_OBJECT_PATH],
        destination_project_dataset_table=f"{PROJECT_ID}.{BQ_STAGING_DATASET}.{BQ_STAGING_TABLE}",
        source_format="CSV",
        skip_leading_rows=1,        # Skip header row
        write_disposition="WRITE_APPEND",  # Append new data each week
        autodetect=False,
        schema_fields=[
            {"name": "Extraction_Date", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Competitor", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Category", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Title", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Price", "type": "STRING", "mode": "NULLABLE"},
            {"name": "URL", "type": "STRING", "mode": "NULLABLE"},
            {"name": "SKU", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Image_URL", "type": "STRING", "mode": "NULLABLE"},
        ],
        google_cloud_conn_id="google_cloud_default",
    )

    # ------------------------------------------
    # Task 3: Transform Staging → Production
    # ------------------------------------------
    transform_to_production = BigQueryInsertJobOperator(
        task_id="transform_staging_to_production",
        configuration={
            "query": {
                "query": f"""
                    MERGE `{PROJECT_ID}.{BQ_PROD_DATASET}.{BQ_PROD_TABLE}` AS target
                    USING (
                        SELECT
                            SAFE.PARSE_DATE('%Y-%m-%d', Extraction_Date) AS extraction_date,
                            Competitor AS competitor,
                            Category AS category,
                            Title AS title,
                            -- Clean price: "R6,999" or "R1,367.00" → numeric
                            SAFE_CAST(
                                REGEXP_REPLACE(
                                    REPLACE(REPLACE(Price, 'R', ''), ' ', ''),
                                    r'[^0-9.]', ''
                                ) AS FLOAT64
                            ) AS price,
                            URL AS url,
                            SKU AS sku,
                            Image_URL AS image_url,
                            -- Dedup: keep the latest entry per SKU + Competitor + Date
                            ROW_NUMBER() OVER (
                                PARTITION BY Competitor, SKU, Extraction_Date
                                ORDER BY Extraction_Date DESC
                            ) AS row_num
                        FROM `{PROJECT_ID}.{BQ_STAGING_DATASET}.{BQ_STAGING_TABLE}`
                        WHERE Extraction_Date = '{EXECUTION_DATE}'
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
                """,
                "useLegacySql": False,
            }
        },
        google_cloud_conn_id="google_cloud_default",
    )

    # ------------------------------------------
    # Task 4: Archive processed CSV
    # ------------------------------------------
    archive_csv = GCSToGCSOperator(
        task_id="archive_processed_csv",
        source_bucket=GCS_BUCKET,
        source_object=GCS_OBJECT_PATH,
        destination_bucket=GCS_BUCKET,
        destination_object=f"archive/{EXECUTION_DATE}/za_pc_parts_master_{EXECUTION_DATE}.csv",
        move_object=True,  # Moves (not copies) the file to archive/
        google_cloud_conn_id="google_cloud_default",
    )

    # ------------------------------------------
    # Task Dependencies
    # ------------------------------------------
    wait_for_csv >> load_to_staging >> transform_to_production >> archive_csv
