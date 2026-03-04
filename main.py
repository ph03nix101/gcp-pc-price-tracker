import os
import time
import pandas as pd
from dotenv import load_dotenv
from firecrawl import Firecrawl
from google.cloud import storage
from google.cloud import bigquery

from config import MASTER_CONFIG
from scrapers import SCRAPER_MAP

# ==========================================
# BigQuery Configuration
# ==========================================
BQ_PROJECT = "dsfsi-486822"
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


def run_master_pipeline(api_key):
    """
    Orchestrates the entire scraping process based on the MASTER_CONFIG.
    Loops through every retailer → category → URL and calls the assigned scraper.
    """
    print("=== INITIALIZING MASTER PC PARTS AGGREGATOR ===")
    app = Firecrawl(api_key=api_key)
    master_dataset = []

    for retailer_name, config_data in MASTER_CONFIG.items():
        print(f"\n[TARGET] Processing {retailer_name}...")
        scraper_func = SCRAPER_MAP[config_data["scraper_function"]]

        for category_name, url_list in config_data["categories"].items():
            if not isinstance(url_list, list) or not url_list:
                print(f"  [!] Skipping {category_name} — invalid config (not a list)")
                continue

            print(f"  -> Category: {category_name}")

            for target_url in url_list:
                if not target_url:
                    continue

                print(f"    - URL: {target_url}")

                try:
                    extracted_items = scraper_func(app, target_url, category_name)
                except Exception as e:
                    print(f"    [!] Scraper crashed: {e}")
                    extracted_items = []

                if extracted_items:
                    master_dataset.extend(extracted_items)
                    print(f"    - Extracted {len(extracted_items)} items.")
                else:
                    print("    - No items extracted from this URL.")

                time.sleep(3)

    print("\n=== EXTRACTION COMPLETE ===")

    df_master = pd.DataFrame(master_dataset)

    if not df_master.empty:
        total_products = len(df_master)
        print(f"Total Database Size: {total_products} products across {len(MASTER_CONFIG)} retailers.")
        
        # --- GCS Upload Logic ---
        bucket_name = os.getenv("GCS_BUCKET_NAME", "za-pc-parts-raw")
        timestamp = pd.Timestamp.now().strftime('%Y-%m-%d')
        filename = f"inbound/{timestamp}/za_pc_parts_master_{timestamp}.csv"
        
        try:
            print(f"Uploading to GCS Bucket: {bucket_name} -> {filename}...")
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(filename)
            
            # Stream directly from memory
            csv_data = df_master.to_csv(index=False)
            blob.upload_from_string(csv_data, content_type="text/csv")
            
            print(f"Successfully saved Master Dataset to GCS.")
        except Exception as e:
            print(f"[!] Failed to upload to GCS: {e}")
            local_filename = f"za_pc_parts_master_{timestamp}.csv"
            df_master.to_csv(local_filename, index=False)
            print(f"Saved locally to {local_filename} instead.")

        # --- BigQuery Staging Load ---
        try:
            print(f"\n=== LOADING INTO BIGQUERY STAGING ===")
            bq_client = bigquery.Client(project=BQ_PROJECT)

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=0,  # No header — loading from DataFrame
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )

            load_job = bq_client.load_table_from_dataframe(
                df_master, BQ_STAGING_TABLE, job_config=job_config
            )
            load_job.result()  # Wait for completion

            print(f"Loaded {load_job.output_rows} rows into {BQ_STAGING_TABLE}")

            # --- Transform Staging → Production ---
            print(f"\n=== TRANSFORMING TO PRODUCTION ===")
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("extraction_date", "STRING", timestamp),
                ]
            )
            query_job = bq_client.query(TRANSFORM_SQL, job_config=job_config, location="europe-west1")
            query_job.result()  # Wait for completion

            print(f"Transform complete. Production table updated.")

            # --- Archive processed CSV in GCS ---
            try:
                source_blob = bucket.blob(filename)
                archive_path = f"archive/{timestamp}/za_pc_parts_master_{timestamp}.csv"
                bucket.rename_blob(source_blob, archive_path)
                print(f"Archived CSV to {archive_path}")
            except Exception as e:
                print(f"[!] Archive step failed (non-critical): {e}")

        except Exception as e:
            print(f"[!] BigQuery pipeline failed: {e}")
            print("Data is still safely stored in GCS for manual recovery.")

    else:
        print("Pipeline finished, but no data was extracted.")


# ==========================================
# Execution (Cloud Run Job / Local)
# ==========================================
if __name__ == "__main__":
    load_dotenv()
    FIRECRAWL_KEY = os.getenv('FIRECRAWL_API_KEY')
    
    if not FIRECRAWL_KEY:
        print("ERROR: FIRECRAWL_API_KEY missing from environment variables.")
        exit(1)
        
    run_master_pipeline(FIRECRAWL_KEY)