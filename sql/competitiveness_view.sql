-- =====================================================
-- Retailer Competitiveness View
-- =====================================================
-- Scores each retailer per category per week based on:
--   60% → Price leadership (how often they are the cheapest)
--   40% → Stock breadth (SKU count relative to category max)
--
-- Connect this view directly to Looker Studio.
-- The category filter on your dashboard will filter this view automatically.
-- =====================================================

CREATE OR REPLACE VIEW `dsfsi-486822.prod_pc_parts.vw_retailer_competitiveness` AS

WITH

-- 1. Get the cheapest price per product title per extraction date
cheapest_per_product AS (
    SELECT
        extraction_date,
        category,
        title,
        MIN(price) AS min_price
    FROM `dsfsi-486822.prod_pc_parts.products`
    WHERE price IS NOT NULL
    GROUP BY 1, 2, 3
),

-- 2. Flag which retailer(s) match the cheapest price for each product
price_leadership AS (
    SELECT
        p.extraction_date,
        p.category,
        p.competitor,
        COUNT(*) AS total_products,
        COUNTIF(p.price = c.min_price) AS times_cheapest
    FROM `dsfsi-486822.prod_pc_parts.products` p
    JOIN cheapest_per_product c
        ON p.title = c.title
        AND p.category = c.category
        AND p.extraction_date = c.extraction_date
    WHERE p.price IS NOT NULL
    GROUP BY 1, 2, 3
),

-- 3. Get SKU count per retailer per category per week
stock_breadth AS (
    SELECT
        extraction_date,
        category,
        competitor,
        COUNT(DISTINCT sku) AS sku_count
    FROM `dsfsi-486822.prod_pc_parts.products`
    GROUP BY 1, 2, 3
),

-- 4. Get the max SKU count per category per week (for normalizing)
max_sku_per_category AS (
    SELECT
        extraction_date,
        category,
        MAX(sku_count) AS max_sku_count
    FROM stock_breadth
    GROUP BY 1, 2
)

-- 5. Combine into final competitiveness score (0–100)
SELECT
    pl.extraction_date,
    pl.category,
    pl.competitor,
    pl.total_products,
    pl.times_cheapest,
    sb.sku_count,

    -- Price leadership ratio (0.0 → 1.0)
    SAFE_DIVIDE(pl.times_cheapest, pl.total_products) AS price_leadership_ratio,

    -- Stock breadth ratio (0.0 → 1.0)
    SAFE_DIVIDE(sb.sku_count, mx.max_sku_count) AS stock_breadth_ratio,

    -- Final weighted competitiveness score (0–100)
    ROUND(
        (SAFE_DIVIDE(pl.times_cheapest, pl.total_products) * 0.6
        + SAFE_DIVIDE(sb.sku_count, mx.max_sku_count) * 0.4) * 100,
        1
    ) AS competitiveness_score

FROM price_leadership pl
JOIN stock_breadth sb
    ON pl.competitor = sb.competitor
    AND pl.category = sb.category
    AND pl.extraction_date = sb.extraction_date
JOIN max_sku_per_category mx
    ON pl.category = mx.category
    AND pl.extraction_date = mx.extraction_date

ORDER BY pl.extraction_date DESC, pl.category, competitiveness_score DESC;
