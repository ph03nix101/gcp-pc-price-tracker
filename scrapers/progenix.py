import time
import re
from bs4 import BeautifulSoup
import pandas as pd


def scrape_progenix(app, base_url, category):
    """
    Scrapes a Progenix (OpenCart) category.
    Uses rawHtml format with a wait action to bypass WAF/Cloudflare.
    """
    all_products = []
    current_page = 1

    while True:
        if current_page > 1:
            separator = '&' if '?' in base_url else '?'
            target_url = f"{base_url}{separator}page={current_page}"
        else:
            target_url = base_url
        print(f"  -> Scraping {category} Page {current_page}...")

        try:
            result = app.scrape(
                target_url,
                formats=["rawHtml"],
                actions=[{"type": "wait", "milliseconds": 3000}]
            )
            raw_html = (
                getattr(result, 'rawHtml', None)
                or getattr(result, 'raw_html', None)
                or getattr(result, 'html', None)
            )
            if not raw_html:
                break
        except Exception as e:
            print(f"  [!] Error: {e}")
            break

        soup = BeautifulSoup(raw_html, 'html.parser')
        product_cards = soup.find_all('div', class_='product-layout')
        if not product_cards:
            break

        for card in product_cards:
            # 1. Title & URL
            title_elem = card.find('h4')
            title = title_elem.find('a').text.strip() if title_elem and title_elem.find('a') else "N/A"
            product_url = title_elem.find('a').get('href', 'N/A') if title_elem and title_elem.find('a') else "N/A"

            # 2. Price
            price_elem = card.find('p', class_='price')
            price_match = re.search(r'R[\d,]+', price_elem.text.replace('\xa0', '')) if price_elem else None
            price = price_match.group(0) if price_match else "N/A"

            # 3. SKU from cart button onclick
            button = card.find('button', onclick=re.compile(r'cart\.add'))
            id_match = re.search(r"cart\.add\('(\d+)'", button.get('onclick', '')) if button else None
            product_id = id_match.group(1) if id_match else "N/A"

            # 4. Image URL
            img_tag = card.find('img', src=True)
            image_url = img_tag.get('src', 'N/A') if img_tag else "N/A"

            if title != "N/A":
                all_products.append({
                    'Extraction_Date': pd.Timestamp.now().strftime('%Y-%m-%d'),
                    'Competitor': 'Progenix',
                    'Category': category,
                    'Title': title,
                    'Price': price,
                    'URL': product_url,
                    'SKU': product_id,
                    'Image_URL': image_url,
                })

        pagination = soup.find('ul', class_='pagination')
        if pagination and pagination.find('a', string='>'):
            current_page += 1
            time.sleep(2)
        else:
            break

    return all_products
