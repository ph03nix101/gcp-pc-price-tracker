import time
import re
from bs4 import BeautifulSoup
import pandas as pd


def scrape_dreamware(app, base_url, category):
    """
    Scrapes a Dreamware Tech category.
    Uses image tag attributes to get full (non-truncated) product titles.
    """
    all_products = []
    current_page = 1

    while True:
        target_url = f"{base_url}?page={current_page}" if current_page > 1 else base_url
        print(f"  -> Scraping {category} Page {current_page}...")

        try:
            result = app.scrape(url=target_url, formats=['html'])

            if isinstance(result, dict):
                raw_html = result.get('html')
            else:
                raw_html = getattr(result, 'html', None)

            if not raw_html:
                print(f"  [!] No HTML returned for page {current_page}")
                break

        except Exception as e:
            print(f"  [!] Firecrawl Error: {e}")
            break

        soup = BeautifulSoup(raw_html, 'html.parser')

        # Target div elements with both 'product' and 'card' classes
        product_cards = soup.find_all(
            'div', class_=lambda x: x and 'product' in x.split() and 'card' in x.split()
        )

        if not product_cards:
            print("  No product cards found. Stopping pagination.")
            break

        for card in product_cards:
            # 1. Title & Image from <img> tag
            img_tag = card.find('img')
            title = img_tag.get('title', '').strip() if img_tag else "N/A"
            image_url = img_tag.get('src', 'N/A') if img_tag else "N/A"

            # 2. Product URL
            name_container = card.find('p', class_='product-box-name')
            if name_container and name_container.find('a'):
                href = name_container.find('a').get('href', '')
                product_url = f"https://www.dreamwaretech.co.za{href}" if href.startswith('/') else href
            else:
                product_url = "N/A"

            # 3. Price
            price_elem = card.find('p', class_='product-price')
            if price_elem:
                raw_price_text = price_elem.text.replace('From', '').strip()
                digits = re.sub(r'[^\d]', '', raw_price_text)
                price = f"R{digits}" if digits else "N/A"
            else:
                price = "N/A"

            # 4. SKU from wishlist button
            wishlist_btn = card.find('a', class_='add-to-wishlist')
            if wishlist_btn and wishlist_btn.get('data-product'):
                product_id = wishlist_btn.get('data-product')
            else:
                product_id = "N/A"

            if title != "N/A" and price != "N/A":
                all_products.append({
                    'Extraction_Date': pd.Timestamp.now().strftime('%Y-%m-%d'),
                    'Competitor': 'Dreamware Tech',
                    'Category': category,
                    'Title': title,
                    'Price': price,
                    'URL': product_url,
                    'SKU': product_id,
                    'Image_URL': image_url,
                })

        # Pagination: look for fa-arrow-right icon
        has_next_page = False
        next_icon = soup.find('i', class_='fa-arrow-right')
        if next_icon:
            parent_link = next_icon.find_parent('a')
            if parent_link and parent_link.get('href'):
                has_next_page = True

        if has_next_page:
            current_page += 1
            time.sleep(2)
        else:
            print(f"  No 'Next' arrow on page {current_page}. Reached the end!")
            break

    return all_products
