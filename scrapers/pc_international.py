import time
from bs4 import BeautifulSoup
import pandas as pd


def scrape_pc_international(app, base_url, category):
    """
    Scrapes a PC International (WooCommerce) category.
    Uses WooCommerce path-based pagination: /page/N/
    """
    all_products = []
    current_page = 1

    while True:
        if current_page == 1:
            target_url = base_url
        else:
            clean_base = base_url.rstrip('/')
            target_url = f"{clean_base}/page/{current_page}/"

        print(f"  -> Scraping {category} Page {current_page}...")

        try:
            result = app.scrape(url=target_url, formats=['html'])

            if isinstance(result, dict):
                raw_html = result.get('html')
            else:
                raw_html = getattr(result, 'html', None)

            if not raw_html:
                print(f"  [!] No HTML returned. Stopping pagination.")
                break

        except Exception as e:
            print(f"  [!] Firecrawl Error (Likely reached the end): {e}")
            break

        soup = BeautifulSoup(raw_html, 'html.parser')

        # WooCommerce product containers
        product_cards = soup.find_all('li', class_='product')

        if not product_cards:
            print("  No product cards found. Reached the end!")
            break

        for card in product_cards:
            # 1. Title and URL
            title_container = card.find('div', class_='product-details_title')
            if title_container and title_container.find('a'):
                a_tag = title_container.find('a')
                title = a_tag.text.strip()
                product_url = a_tag.get('href', '').strip()
            else:
                title, product_url = "N/A", "N/A"

            # 2. Price
            price_elem = card.find('span', class_='woocommerce-Price-amount')
            if price_elem:
                price = price_elem.text.strip()
            else:
                price = "N/A"

            # 3. SKU
            sku_elem = card.find('div', class_='product-details_sku')
            if sku_elem:
                product_id = sku_elem.text.replace('SKU:', '').strip()
            else:
                product_id = "N/A"

            # 4. Image URL
            img_tag = card.find('img', src=True)
            image_url = img_tag.get('src', 'N/A') if img_tag else "N/A"

            if title != "N/A" and price != "N/A":
                all_products.append({
                    'Extraction_Date': pd.Timestamp.now().strftime('%Y-%m-%d'),
                    'Competitor': 'PC International',
                    'Category': category,
                    'Title': title,
                    'Price': price,
                    'URL': product_url,
                    'SKU': product_id,
                    'Image_URL': image_url,
                })

        current_page += 1
        time.sleep(2)

    return all_products
