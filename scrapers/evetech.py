import time
import re
from bs4 import BeautifulSoup
import pandas as pd


def scrape_evetech(app, base_url, category):
    """
    Scrapes a complete Evetech category dynamically.
    Updated for the new JSX-based product card structure (2026).
    Handles both Component cards (GPUs, CPUs, etc.) and Laptop cards.
    """
    all_products = []
    current_page = 1

    while True:
        target_url = f"{base_url}?page={current_page}" if current_page > 1 else base_url
        print(f"  -> Scraping {category} Page {current_page}...")

        try:
            result = app.scrape(
                url=target_url,
                formats=['html'],
                timeout=30000
            )

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

        # --- NEW STRUCTURE: div.product-card containers ---
        product_cards = soup.find_all('div', class_='product-card')
        products_found_on_page = 0

        if not product_cards:
            print("  No product cards found on this page. Reached the end.")
            break

        for card in product_cards:
            # 1. Extract Title & URL from the <a> tag that wraps the <h3>
            title_link = card.find('a', attrs={'title': True, 'href': True})
            if not title_link or not title_link.find('h3'):
                for a_tag in card.find_all('a', href=True):
                    if a_tag.find('h3'):
                        title_link = a_tag
                        break
                if not title_link or not title_link.find('h3'):
                    continue

            title = title_link.get('title', '').strip()
            href = title_link.get('href', '')

            # Fallback to span text inside h3
            if not title:
                h3_span = title_link.find('span')
                title = h3_span.text.strip() if h3_span else "N/A"

            if not title or title == "N/A":
                continue

            product_url = f"https://www.evetech.co.za{href}" if href.startswith('/') else href

            # 2. Extract Product ID from last URL path segment
            url_segments = [s for s in href.split('/') if s]
            product_id = url_segments[-1] if url_segments and url_segments[-1].isdigit() else "N/A"

            if product_id == "N/A":
                continue

            # 3. Extract Price from div with 'font-semibold' and 'whitespace-nowrap'
            price = "N/A"
            price_divs = card.find_all('div', class_=lambda x: x and 'font-semibold' in x and 'whitespace-nowrap' in x)
            for price_div in price_divs:
                price_text = price_div.get_text(strip=True)
                price_match = re.search(r'R\s*[\d,]+', price_text)
                if price_match:
                    price = price_match.group(0).replace(' ', '')
                    break

            # Fallback: broader scan
            if price == "N/A":
                all_text_divs = card.find_all('div', string=re.compile(r'R\s*[\d,]+'))
                for div in all_text_divs:
                    price_match = re.search(r'R\s*[\d,]+', div.text)
                    if price_match:
                        price = price_match.group(0).replace(' ', '')
                        break

            if price == "N/A":
                continue

            # 4. Extract Image URL
            image_url = "N/A"
            img_tag = card.find('img', src=True)
            if img_tag:
                image_url = img_tag.get('src', 'N/A')

            all_products.append({
                'Extraction_Date': pd.Timestamp.now().strftime('%Y-%m-%d'),
                'Competitor': 'Evetech',
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

        # --- PAGINATION: SVG chevron path ---
        has_next_page = False
        pagination_buttons = soup.find_all('button', class_=lambda x: x and 'cursor-pointer' in x)
        for button in pagination_buttons:
            svg = button.find('svg')
            if svg:
                path = svg.find('path')
                if path and path.get('d') == 'M9 5l7 7-7 7':
                    has_next_page = True
                    break

        if has_next_page:
            current_page += 1
            time.sleep(2)
        else:
            print(f"  No 'Next' button found on page {current_page}. Reached the end!")
            break

    # Deduplicate by SKU
    seen_skus = set()
    unique_products = []
    for p in all_products:
        if p['SKU'] not in seen_skus:
            seen_skus.add(p['SKU'])
            unique_products.append(p)

    return unique_products
