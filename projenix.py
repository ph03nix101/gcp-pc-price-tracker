import os
from firecrawl import Firecrawl
from bs4 import BeautifulSoup
import pandas as pd
import time
import re


def scrape_progenix_category_firecrawl(base_url, api_key):
    """
    Scrapes a Progenix (OpenCart) category.
    Includes logic to extract IDs from JavaScript strings.
    """
    api_key= "fc-3d6d051e3cd54e33a9f5a6e934a8e83a"  # Replace with your actual Firecrawl API key
    app = Firecrawl(api_key=api_key)
    all_products = []
    current_page = 1

    while True:
        # Standard URL query pagination: ?page=x
        target_url = f"{base_url}?page={current_page}" if current_page > 1 else base_url
        print(f"\n--- Scraping Page {current_page} via Firecrawl: {target_url} ---")

        try:
            # Added a longer timeout (30 seconds) to allow Cloudflare challenges to pass
            result = app.scrape(
                url=target_url,
                formats=['html'],
                timeout=30000
            )
            raw_html = result.get('html') if isinstance(result, dict) else getattr(result, 'html', None)

            if not raw_html:
                print(f"[!] No HTML returned for page {current_page}")
                break
        except Exception as e:
            print(f"[!] Firecrawl Error: {e}")
            break

        soup = BeautifulSoup(raw_html, 'html.parser')

        # --- DEBUGGING ADDITION ---
        page_title = soup.title.text.strip() if soup.title else 'No Title'
        print(f"[*] Page Title Found: {page_title}")

        # If we hit an issue, save the HTML so we can see what Firecrawl saw
        if not soup.find_all('div', class_='product-layout'):
            print("[!] Saving error HTML to debug_progenix.html...")
            with open("debug_progenix.html", "w", encoding="utf-8") as f:
                f.write(raw_html)
        # --------------------------

        product_cards = soup.find_all('div', class_='product-layout')

        soup = BeautifulSoup(raw_html, 'html.parser')
        product_cards = soup.find_all('div', class_='product-layout')

        if not product_cards:
            print("No product cards found. Reached the end!")
            break

        for card in product_cards:
            # 1. Extract Title & URL
            title_elem = card.find('h4')
            if title_elem and title_elem.find('a'):
                title = title_elem.find('a').text.strip()
                href = title_elem.find('a').get('href', '')
                product_url = href
            else:
                title, product_url = "N/A", "N/A"

            # 2. Extract Price (Clean out whitespace and newlines)
            price_elem = card.find('p', class_='price')
            if price_elem:
                # regex to grab 'R' followed by digits and commas (ignores 'Ex Tax' text)
                price_match = re.search(r'R[\d,]+', price_elem.text.replace('\xa0', ''))
                price = price_match.group(0) if price_match else "N/A"
            else:
                price = "N/A"

            # 3. Extract SKU (Internal ID from the Cart button)
            # Looks for cart.add('9187', '1')
            button = card.find('button', onclick=re.compile(r'cart\.add'))
            if button:
                onclick_text = button.get('onclick', '')
                id_match = re.search(r"cart\.add\('(\d+)'", onclick_text)
                product_id = id_match.group(1) if id_match else "N/A"
            else:
                product_id = "N/A"

            # 4. Extract Image URL from product thumbnail
            img_tag = card.find('img', src=True)
            image_url = img_tag.get('src', 'N/A') if img_tag else "N/A"

            if title != "N/A" and price != "N/A":
                all_products.append({
                    'Competitor': 'Progenix',
                    'Title': title,
                    'Price': price,
                    'URL': product_url,
                    'SKU': product_id,
                    'Image_URL': image_url,
                    'Extraction_Date': pd.Timestamp.now().strftime('%Y-%m-%d')
                })

        # --- PAGINATION CHECK ---
        # Look for the '>' (Next) link in the pagination list
        next_link = soup.find('ul', class_='pagination')
        if next_link:
            next_page_btn = next_link.find('a', string='>')
            if next_page_btn:
                current_page += 1
                time.sleep(2)
            else:
                break
        else:
            break

    return pd.DataFrame(all_products)


# --- Execution ---
TARGET_URL = "https://progenix.co.za/Components/Graphics-Cards/refine/stock_status,7"
FIRECRAWL_KEY = "fc-YOUR_KEY_HERE"

df_progenix = scrape_progenix_category_firecrawl(TARGET_URL, FIRECRAWL_KEY)
print(f"Scraped {len(df_progenix)} products.")
print(df_progenix.head())