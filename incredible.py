import os
from firecrawl import Firecrawl
from bs4 import BeautifulSoup
import pandas as pd
import time
import urllib.parse


def scrape_incredible_category_firecrawl(base_url, api_key):
    """
    Scrapes an Incredible Connection (Magento 2) category.
    Handles existing URL parameters (like ?cat=...) when paginating.
    """
    api_key="fc-3d6d051e3cd54e33a9f5a6e934a8e83a"  # Replace with your actual Firecrawl API key
    app = Firecrawl(api_key=api_key)
    all_products = []
    current_page = 1

    while True:
        # Handle pagination cleanly, preserving any existing URL parameters
        if current_page == 1:
            target_url = base_url
        else:
            # Check if there's already a '?' in the URL (like ?cat=123)
            separator = '&' if '?' in base_url else '?'
            target_url = f"{base_url}{separator}p={current_page}"

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

        # 1. Target the Magento product item container
        product_cards = soup.find_all('li', class_='product-item')

        if not product_cards:
            print("No product cards found on this page. Stopping pagination.")
            break

        for card in product_cards:
            # 2. Extract Title and URL
            title_elem = card.find('strong', class_='product-item-name')
            if title_elem and title_elem.find('a'):
                a_tag = title_elem.find('a')
                title = a_tag.text.strip()
                product_url = a_tag.get('href', '').strip()
            else:
                title, product_url = "N/A", "N/A"

            # 3. Extract Price
            # Magento stores the raw integer amount in 'data-price-amount'
            price_elem = card.find('span', class_='price-wrapper')
            if price_elem and price_elem.get('data-price-amount'):
                # Grab the raw amount, e.g., "2899", and format it
                raw_amount = price_elem.get('data-price-amount')
                # Ensure it formats nicely even if it has decimals
                try:
                    price = f"R{int(float(raw_amount)):,}"
                except ValueError:
                    price = f"R{raw_amount}"
            else:
                # Fallback to the text itself if the data attribute is missing
                price_text = card.find('span', class_='price')
                price = price_text.text.strip().replace(' ', '').replace('\xa0', '') if price_text else "N/A"

            # 4. Extract SKU/Product ID
            # Grab from the price box data attribute
            price_box = card.find('div', class_='price-box')
            if price_box and price_box.get('data-product-id'):
                product_id = price_box.get('data-product-id')
            else:
                product_id = "N/A"

            # 5. Extract Image URL (Magento 2 product-image-photo class)
            img_tag = card.find('img', class_='product-image-photo')
            if not img_tag:
                img_tag = card.find('img', src=True)  # Fallback to any img
            image_url = img_tag.get('src', 'N/A') if img_tag else "N/A"

            # Only append valid products
            if title != "N/A" and price != "N/A":
                all_products.append({
                    'Competitor': 'Incredible Connection',
                    'Title': title,
                    'Price': price,
                    'URL': product_url,
                    'SKU': product_id,
                    'Image_URL': image_url,
                    'Extraction_Date': pd.Timestamp.now().strftime('%Y-%m-%d')
                })

        # --- DYNAMIC PAGINATION CHECK ---
        # Look for the 'next' list item in the pagination ul
        next_button = soup.find('li', class_='pages-item-next')

        # Check if the next button exists AND has an active anchor tag
        if next_button and next_button.find('a', class_='next'):
            current_page += 1
            time.sleep(2)
        else:
            print(f"\nNo valid 'Next' button found on page {current_page}. Reached the end!")
            break

    return pd.DataFrame(all_products)


# --- Execution ---
# Note: I am using the components category link you provided that has the ?cat parameter
TARGET_CATEGORY_URL = "https://www.incredible.co.za/products/gaming/components?cat=367572"
FIRECRAWL_API_KEY = "fc-YOUR_FIRECRAWL_KEY_HERE"  # Put your key here

df_incredible = scrape_incredible_category_firecrawl(TARGET_CATEGORY_URL, FIRECRAWL_API_KEY)

print("\n--- Scraping Complete ---")
print(f"Total Products Scraped: {len(df_incredible)}")
print(df_incredible.head())

if not df_incredible.empty:
    df_incredible.to_csv("incredible_raw_extract.csv", index=False)
    print("Data saved successfully to incredible_raw_extract.csv!")