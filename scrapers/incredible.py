import time
from bs4 import BeautifulSoup
import pandas as pd


def scrape_incredible(app, base_url, category):
    """
    Scrapes an Incredible Connection (Magento 2) category.
    Handles existing URL parameters (like ?cat=...) when paginating.
    """
    all_products = []
    current_page = 1

    while True:
        if current_page == 1:
            target_url = base_url
        else:
            separator = '&' if '?' in base_url else '?'
            target_url = f"{base_url}{separator}p={current_page}"

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

        # Magento 2 product item containers
        product_cards = soup.find_all('li', class_='product-item')

        if not product_cards:
            print("  No product cards found. Stopping pagination.")
            break

        for card in product_cards:
            # 1. Title and URL
            title_elem = card.find('strong', class_='product-item-name')
            if title_elem and title_elem.find('a'):
                a_tag = title_elem.find('a')
                title = a_tag.text.strip()
                product_url = a_tag.get('href', '').strip()
            else:
                title, product_url = "N/A", "N/A"

            # 2. Price from data-price-amount attribute
            price_elem = card.find('span', class_='price-wrapper')
            if price_elem and price_elem.get('data-price-amount'):
                raw_amount = price_elem.get('data-price-amount')
                try:
                    price = f"R{int(float(raw_amount)):,}"
                except ValueError:
                    price = f"R{raw_amount}"
            else:
                price_text = card.find('span', class_='price')
                price = price_text.text.strip().replace(' ', '').replace('\xa0', '') if price_text else "N/A"

            # 3. SKU from price-box data attribute
            price_box = card.find('div', class_='price-box')
            if price_box and price_box.get('data-product-id'):
                product_id = price_box.get('data-product-id')
            else:
                product_id = "N/A"

            # 4. Image URL (Magento 2 product-image-photo)
            img_tag = card.find('img', class_='product-image-photo')
            if not img_tag:
                img_tag = card.find('img', src=True)
            image_url = img_tag.get('src', 'N/A') if img_tag else "N/A"

            if title != "N/A" and price != "N/A":
                all_products.append({
                    'Extraction_Date': pd.Timestamp.now().strftime('%Y-%m-%d'),
                    'Competitor': 'Incredible Connection',
                    'Category': category,
                    'Title': title,
                    'Price': price,
                    'URL': product_url,
                    'SKU': product_id,
                    'Image_URL': image_url,
                })

        # Pagination: Magento 2 next button
        next_button = soup.find('li', class_='pages-item-next')
        if next_button and next_button.find('a', class_='next'):
            current_page += 1
            time.sleep(2)
        else:
            print(f"  No 'Next' button on page {current_page}. Reached the end!")
            break

    return all_products
