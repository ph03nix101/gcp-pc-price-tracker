-- ==========================================
-- BigQuery Dataset & Table Setup
-- Run these in BigQuery Console or via bq CLI
-- ==========================================

-- 1. Create Staging Dataset
CREATE SCHEMA IF NOT EXISTS `dsfsi-486822.stg_pc_parts`
OPTIONS (
    location = 'europe-west1',
    description = 'Raw scraped PC parts data from GCS CSV files'
);

-- 2. Create Staging Table (all STRING — mirrors CSV exactly)
CREATE TABLE IF NOT EXISTS `dsfsi-486822.stg_pc_parts.raw_extracts` (
    Extraction_Date STRING,
    Competitor STRING,
    Category STRING,
    Title STRING,
    Price STRING,
    URL STRING,
    SKU STRING,
    Image_URL STRING
);

-- 3. Create Production Dataset
CREATE SCHEMA IF NOT EXISTS `dsfsi-486822.prod_pc_parts`
OPTIONS (
    location = 'europe-west1',
    description = 'Clean, typed PC parts pricing data for Looker Studio'
);

-- 4. Create Production Table (properly typed for analytics)
CREATE TABLE IF NOT EXISTS `dsfsi-486822.prod_pc_parts.products` (
    extraction_date DATE,
    competitor STRING,
    category STRING,
    title STRING,
    price FLOAT64,
    url STRING,
    sku STRING,
    image_url STRING
)
PARTITION BY extraction_date
CLUSTER BY competitor, category
OPTIONS (
    description = 'Cleaned PC parts pricing data partitioned by extraction date. Connect this table to Looker Studio.'
);
