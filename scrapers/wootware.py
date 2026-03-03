import time
from bs4 import BeautifulSoup
import pandas as pd


def scrape_wootware(app, base_url, category):
    """
    Scrapes a Wootware (Magento 1) category.
    Handles pagination via ?p=N query parameter.
    """
    all_products = []
    current_page = 1

    while True:
        target_url = f"{base_url}?p={current_page}" if current_page > 1 else base_url
        print(f"  -> Scraping {category} Page {current_page}...")

        try:
            result = app.scrape(url=target_url, formats=['html'])
            raw_html = result.get('html') if isinstance(result, dict) else getattr(result, 'html', None)
            if not raw_html:
                break
        except Exception as e:
            print(f"  [!] Error: {e}")
            break

        soup = BeautifulSoup(raw_html, 'html.parser')
        product_cards = soup.find_all('li', class_='item')
        if not product_cards:
            break

        for card in product_cards:
            title_elem = card.find('h2', class_='product-name')
            title = title_elem.find('a').get('title', '').strip() if title_elem and title_elem.find('a') else "N/A"
            product_url = title_elem.find('a').get('href', '').strip() if title_elem and title_elem.find('a') else "N/A"

            price_elem = card.find('span', id=lambda x: x and x.startswith('product-price-'))
            price = price_elem.text.strip().replace(' ', '') if price_elem else "N/A"
            product_id = price_elem.get('id', '').split('-')[-1] if price_elem else "N/A"

            img_tag = card.find('img', src=True)
            image_url = img_tag.get('src', 'N/A') if img_tag else "N/A"

            if title != "N/A" and price != "N/A":
                all_products.append({
                    'Extraction_Date': pd.Timestamp.now().strftime('%Y-%m-%d'),
                    'Competitor': 'Wootware',
                    'Category': category,
                    'Title': title,
                    'Price': price,
                    'URL': product_url,
                    'SKU': product_id,
                    'Image_URL': image_url,
                })

        if soup.find('a', class_='next i-next'):
            current_page += 1
            time.sleep(2)
        else:
            break

    return all_products
