import time
import re
from bs4 import BeautifulSoup
import pandas as pd


def scrape_computermania(app, base_url, category):
    """
    Scrapes a Computer Mania (Shopify) category.
    Uses Shopify's standard collection page structure with ?page=N pagination.
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

        # Shopify product cards — try multiple common Shopify selectors
        product_cards = soup.find_all('div', class_='product-card')
        if not product_cards:
            product_cards = soup.find_all('div', class_='grid-product')
        if not product_cards:
            product_cards = soup.find_all('div', class_='product-item')
        if not product_cards:
            # Broad fallback: look for product links within a grid
            product_cards = soup.find_all('div', class_=lambda x: x and 'product' in x.lower())

        if not product_cards:
            print("  No product cards found. Stopping pagination.")
            break

        products_found_on_page = 0

        for card in product_cards:
            # 1. Title & URL — look for <a> with product link
            title = "N/A"
            product_url = "N/A"
            product_id = "N/A"

            # Try finding a product link (Shopify uses /products/ URLs)
            product_link = None
            for a_tag in card.find_all('a', href=True):
                href = a_tag.get('href', '')
                if '/products/' in href:
                    product_link = a_tag
                    break

            if product_link:
                href = product_link.get('href', '')
                # Title from link text, title attribute, or aria-label
                title = (
                    product_link.get('title', '').strip()
                    or product_link.get('aria-label', '').strip()
                    or product_link.get_text(strip=True)
                )
                # Build full URL
                if href.startswith('/'):
                    product_url = f"https://computermania.co.za{href}"
                else:
                    product_url = href
                # SKU = product slug from URL
                slug_match = re.search(r'/products/([^/?#]+)', href)
                product_id = slug_match.group(1) if slug_match else "N/A"

            if not title or title == "N/A":
                continue

            # 2. Price — look for money/price elements
            price = "N/A"
            # Shopify commonly uses span.money or span.price
            price_elem = card.find('span', class_='money')
            if not price_elem:
                price_elem = card.find('span', class_='price')
            if not price_elem:
                price_elem = card.find(string=re.compile(r'R\s*[\d,]+\.?\d*'))

            if price_elem:
                price_text = price_elem if isinstance(price_elem, str) else price_elem.get_text(strip=True)
                price_match = re.search(r'R\s*[\d,]+\.?\d*', price_text)
                if price_match:
                    price = price_match.group(0).replace(' ', '')
            else:
                # Broader search within the card
                card_text = card.get_text()
                price_match = re.search(r'R\s*[\d,]+\.\d{2}', card_text)
                if price_match:
                    price = price_match.group(0).replace(' ', '')

            if price == "N/A":
                continue

            # 3. Image URL
            img_tag = card.find('img', src=True)
            image_url = "N/A"
            if img_tag:
                # Shopify often uses data-src for lazy loading
                image_url = img_tag.get('data-src', '') or img_tag.get('src', 'N/A')
                # Shopify CDN URLs often start with // (protocol-relative)
                if image_url.startswith('//'):
                    image_url = f"https:{image_url}"

            all_products.append({
                'Extraction_Date': pd.Timestamp.now().strftime('%Y-%m-%d'),
                'Competitor': 'Computer Mania',
                'Category': category,
                'Title': title,
                'Price': price,
                'URL': product_url,
                'SKU': product_id,
                'Image_URL': image_url,
            })
            products_found_on_page += 1

        print(f"    Found {products_found_on_page} products on page {current_page}.")

        if products_found_on_page == 0:
            break

        # Shopify pagination — check for next page link
        has_next_page = False

        # Method 1: Look for pagination <a> with 'next' or '›' or '»'
        pagination = soup.find('div', class_=lambda x: x and 'pagination' in x.lower()) or soup.find('ul', class_='pagination')
        if pagination:
            next_link = pagination.find('a', string=re.compile(r'(Next|›|»|→)', re.I))
            if not next_link:
                next_link = pagination.find('a', class_=lambda x: x and 'next' in x.lower())
            if next_link:
                has_next_page = True

        # Method 2: Broader search for a 'next' pagination link
        if not has_next_page:
            next_link = soup.find('a', class_=lambda x: x and 'next' in str(x).lower(), href=True)
            if next_link and 'page=' in next_link.get('href', ''):
                has_next_page = True

        if has_next_page:
            current_page += 1
            time.sleep(2)
        else:
            print(f"  No 'Next' link on page {current_page}. Reached the end!")
            break

    return all_products
