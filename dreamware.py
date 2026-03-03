import os
from firecrawl import Firecrawl
from bs4 import BeautifulSoup
import pandas as pd
import time
import re


def scrape_dreamware_category_firecrawl(base_url, api_key):
    """
    Scrapes a Dreamware Tech category using Firecrawl.
    Bypasses truncated titles by extracting from image attributes.
    """
    api_key="fc-3d6d051e3cd54e33a9f5a6e934a8e83a"  # Replace with your actual Firecrawl API key
    app = Firecrawl(api_key=api_key)
    all_products = []
    current_page = 1

    while True:
        # Standard URL query pagination
        target_url = f"{base_url}?page={current_page}" if current_page > 1 else base_url
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
                print(f"[!] No HTML returned from Firecrawl for page {current_page}")
                break

        except Exception as e:
            print(f"[!] Firecrawl Error: {e}")
            break

        soup = BeautifulSoup(raw_html, 'html.parser')

        # 1. Target the specific product card container
        product_cards = soup.find_all('div', class_=lambda x: x and 'product' in x.split() and 'card' in x.split())

        if not product_cards:
            print("No product cards found on this page. Stopping pagination.")
            break

        for card in product_cards:
            # 2. Extract Full Title and Image URL from the Image Tag
            img_tag = card.find('img')
            title = img_tag.get('title', '').strip() if img_tag else "N/A"
            image_url = img_tag.get('src', 'N/A') if img_tag else "N/A"

            # Extract URL from the title container
            name_container = card.find('p', class_='product-box-name')
            if name_container and name_container.find('a'):
                href = name_container.find('a').get('href', '')
                product_url = f"https://www.dreamwaretech.co.za{href}" if href.startswith('/') else href
            else:
                product_url = "N/A"

            # 3. Extract Price
            price_elem = card.find('p', class_='product-price')
            if price_elem:
                # Extracts 'From R20759', removes 'From', 'R', and spaces to isolate the number
                # Then we format it back to 'R20759' for consistency
                raw_price_text = price_elem.text.replace('From', '').strip()
                # Use regex to just get the digits in case of weird formatting
                digits = re.sub(r'[^\d]', '', raw_price_text)
                price = f"R{digits}" if digits else "N/A"
            else:
                price = "N/A"

            # 4. Extract SKU from the hidden wishlist data attribute
            wishlist_btn = card.find('a', class_='add-to-wishlist')
            if wishlist_btn and wishlist_btn.get('data-product'):
                product_id = wishlist_btn.get('data-product')
            else:
                product_id = "N/A"

            # Only append valid products
            if title != "N/A" and price != "N/A":
                all_products.append({
                    'Competitor': 'Dreamware Tech',
                    'Title': title,
                    'Price': price,
                    'URL': product_url,
                    'SKU': product_id,
                    'Image_URL': image_url,
                    'Extraction_Date': pd.Timestamp.now().strftime('%Y-%m-%d')
                })

        # --- DYNAMIC PAGINATION CHECK ---
        has_next_page = False

        # Look for the 'next' arrow icon specifically, because their IDs are messy
        next_icon = soup.find('i', class_='fa-arrow-right')
        if next_icon:
            # Check if it's wrapped in an active link
            parent_link = next_icon.find_parent('a')
            if parent_link and parent_link.get('href'):
                has_next_page = True

        if has_next_page:
            current_page += 1
            time.sleep(2)
        else:
            print(f"\nNo 'Next' arrow found on page {current_page}. Reached the end!")
            break

    return pd.DataFrame(all_products)


# --- Execution ---
TARGET_CATEGORY_URL = "https://www.dreamwaretech.co.za/c/computer-components/graphics-cards-gpus/nvidia-graphics-cards/"
FIRECRAWL_API_KEY = "fc-YOUR_FIRECRAWL_KEY_HERE"  # Put your key here

df_dreamware = scrape_dreamware_category_firecrawl(TARGET_CATEGORY_URL, FIRECRAWL_API_KEY)

print("\n--- Scraping Complete ---")
print(f"Total Products Scraped: {len(df_dreamware)}")
print(df_dreamware.head())

if not df_dreamware.empty:
    df_dreamware.to_csv("dreamware_raw_extract.csv", index=False)
    print("Data saved successfully to dreamware_raw_extract.csv!")