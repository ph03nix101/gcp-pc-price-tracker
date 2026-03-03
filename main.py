import os
import time
import pandas as pd
from dotenv import load_dotenv
from firecrawl import Firecrawl
from google.cloud import storage

from config import MASTER_CONFIG
from scrapers import SCRAPER_MAP


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
            # Fallback to local disk if GCS fails (useful for local testing)
            local_filename = f"za_pc_parts_master_{timestamp}.csv"
            df_master.to_csv(local_filename, index=False)
            print(f"Saved locally to {local_filename} instead.")
            
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