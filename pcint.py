import os
from firecrawl import Firecrawl
from bs4 import BeautifulSoup
import pandas as pd
import time


def scrape_pc_international_firecrawl(base_category_url, api_key):
    """
    Scrapes a PC International category using Firecrawl.
    Handles WooCommerce/FacetWP HTML structures.
    """
    api_key = "fc-3d6d051e3cd54e33a9f5a6e934a8e83a"  # Replace with your actual Firecrawl API key
    app = Firecrawl(api_key=api_key)
    all_products = []
    current_page = 1

    while True:
        # WooCommerce standard pagination format: base_url/page/2/
        if current_page == 1:
            target_url = base_category_url
        else:
            # Ensure base_url doesn't end with a trailing slash before appending
            clean_base = base_category_url.rstrip('/')
            target_url = f"{clean_base}/page/{current_page}/"

        print(f"\n--- Scraping Page {current_page} via Firecrawl: {target_url} ---")

        try:
            result = app.scrape(
                url=target_url,
                formats=['html']
            )

            if isinstance(result, dict):
                raw_html = result.get('html')
            else:
                raw_html = getattr(result, 'html', None)

            if not raw_html:
                print(f"[!] No HTML returned. Stopping pagination.")
                break

        except Exception as e:
            # If we request /page/15/ and it doesn't exist, it might throw a 404 error here.
            print(f"[!] Firecrawl Error (Likely reached the end): {e}")
            break

        soup = BeautifulSoup(raw_html, 'html.parser')

        # 1. Target the PC Int product container
        product_cards = soup.find_all('li', class_='product')

        # If the page loads but has no products, we've reached the end
        if not product_cards:
            print("No product cards found on this page. Reached the end of the category!")
            break

        for card in product_cards:
            # 2. Extract Title and URL from the product details section
            title_container = card.find('div', class_='product-details_title')
            if title_container and title_container.find('a'):
                a_tag = title_container.find('a')
                title = a_tag.text.strip()
                product_url = a_tag.get('href', '').strip()
            else:
                title, product_url = "N/A", "N/A"

            # 3. Extract Price
            price_elem = card.find('span', class_='woocommerce-Price-amount')
            if price_elem:
                # This will grab 'R1,367.00'
                price = price_elem.text.strip()
            else:
                price = "N/A"

            # 4. Extract SKU
            sku_elem = card.find('div', class_='product-details_sku')
            if sku_elem:
                # The text is "SKU: BX80715G6900", so we replace and strip to get just the ID
                product_id = sku_elem.text.replace('SKU:', '').strip()
            else:
                product_id = "N/A"

            # 5. Extract Image URL from product card
            img_tag = card.find('img', src=True)
            image_url = img_tag.get('src', 'N/A') if img_tag else "N/A"

            # Filter out items without prices or titles
            if title != "N/A" and price != "N/A":
                all_products.append({
                    'Competitor': 'PC International',
                    'Title': title,
                    'Price': price,
                    'URL': product_url,
                    'SKU': product_id,
                    'Image_URL': image_url,
                    'Extraction_Date': pd.Timestamp.now().strftime('%Y-%m-%d')
                })

        # Increment page and delay
        current_page += 1
        time.sleep(2)

    return pd.DataFrame(all_products)


# --- Execution ---
# Replace with an actual PC International category URL (e.g., Processors)
TARGET_CATEGORY_URL = "https://pcinternational.co.za/product-category/computer-components/processors/"
FIRECRAWL_API_KEY = "fc-YOUR_FIRECRAWL_KEY_HERE"  # Put your key here

df_pc_int = scrape_pc_international_firecrawl(TARGET_CATEGORY_URL, FIRECRAWL_API_KEY)

print("\n--- Scraping Complete ---")
print(f"Total Products Scraped: {len(df_pc_int)}")
print(df_pc_int.head())

if not df_pc_int.empty:
    df_pc_int.to_csv("pc_int_raw_extract.csv", index=False)
    print("Data saved successfully to pc_int_raw_extract.csv!")