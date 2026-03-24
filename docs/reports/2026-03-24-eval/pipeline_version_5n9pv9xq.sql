ATTACH DATABASE '/Users/perssonisak/Projects/private/self-updating-database/.data/source-datasets-fresh.sqlite' AS source;

DROP TABLE IF EXISTS transactions_clean;
CREATE TABLE transactions_clean AS
WITH normalized AS (
  SELECT
    UPPER(TRIM(transactionId)) AS transaction_id,
    UPPER(TRIM(basketId)) AS basket_id,
    CASE
      WHEN NULLIF(TRIM(businessDate), '') IS NULL THEN NULL
      WHEN date(TRIM(businessDate)) IS NOT NULL THEN date(TRIM(businessDate))
      ELSE TRIM(businessDate)
    END AS business_date,
    CASE
      WHEN NULLIF(TRIM(transactionTimestamp), '') IS NULL THEN NULL
      WHEN datetime(REPLACE(TRIM(transactionTimestamp), 'T', ' ')) IS NOT NULL THEN datetime(REPLACE(TRIM(transactionTimestamp), 'T', ' '))
      ELSE TRIM(transactionTimestamp)
    END AS transaction_timestamp,
    UPPER(TRIM(storeId)) AS store_id,
    UPPER(TRIM(posTerminalId)) AS pos_terminal_id,
    TRIM(receiptNumber) AS receipt_number,
    CAST(lineNumber AS INTEGER) AS line_number,
    UPPER(TRIM(cashierId)) AS cashier_id,
    CASE LOWER(TRIM(channel))
      WHEN 'in-store' THEN 'In-Store'
      WHEN 'instore' THEN 'In-Store'
      WHEN 'click & collect' THEN 'Click & Collect'
      WHEN 'click and collect' THEN 'Click & Collect'
      WHEN 'delivery' THEN 'Delivery'
      ELSE TRIM(channel)
    END AS channel,
    CASE LOWER(TRIM(paymentMethod))
      WHEN 'cash' THEN 'Cash'
      WHEN 'card' THEN 'Card'
      WHEN 'gift card' THEN 'Gift Card'
      WHEN 'giftcard' THEN 'Gift Card'
      WHEN 'mobile wallet' THEN 'Mobile Wallet'
      WHEN 'mobilewallet' THEN 'Mobile Wallet'
      ELSE TRIM(paymentMethod)
    END AS payment_method,
    CASE LOWER(TRIM(loyaltyTier))
      WHEN 'none' THEN 'None'
      WHEN 'bronze' THEN 'Bronze'
      WHEN 'silver' THEN 'Silver'
      WHEN 'gold' THEN 'Gold'
      ELSE TRIM(loyaltyTier)
    END AS loyalty_tier,
    UPPER(TRIM(itemSku)) AS item_sku,
    TRIM(itemDescription) AS item_description,
    CASE LOWER(TRIM(department))
      WHEN 'beverage' THEN 'Beverages'
      ELSE TRIM(department)
    END AS department,
    CASE LOWER(TRIM(category))
      WHEN 'beverages' THEN 'Beverages'
      WHEN 'snacks' THEN 'Snacks'
      WHEN 'produce' THEN 'Produce'
      WHEN 'dairy' THEN 'Dairy'
      WHEN 'bakery' THEN 'Bakery'
      WHEN 'cleaning' THEN 'Cleaning'
      WHEN 'alcohol' THEN 'Alcohol'
      ELSE TRIM(category)
    END AS category,
    TRIM(brand) AS brand,
    CAST(units AS NUMERIC) AS units,
    CAST(unitPriceInclVat AS NUMERIC) AS unit_price_incl_vat,
    CAST(unitPriceExVat AS NUMERIC) AS unit_price_ex_vat,
    CAST(lineDiscountInclVat AS NUMERIC) AS line_discount_incl_vat,
    CAST(vatRate AS NUMERIC) AS vat_rate,
    CAST(netSalesExVat AS NUMERIC) AS net_sales_ex_vat,
    CAST(vatAmount AS NUMERIC) AS vat_amount,
    CAST(grossSalesInclVat AS NUMERIC) AS gross_sales_incl_vat,
    CAST(cogsExVat AS NUMERIC) AS cogs_ex_vat,
    CAST(grossMarginExVat AS NUMERIC) AS gross_margin_ex_vat,
    CASE LOWER(TRIM(returnFlag))
      WHEN 'yes' THEN 'Yes'
      WHEN 'y' THEN 'Yes'
      WHEN 'true' THEN 'Yes'
      WHEN '1' THEN 'Yes'
      WHEN 'no' THEN 'No'
      WHEN 'n' THEN 'No'
      WHEN 'false' THEN 'No'
      WHEN '0' THEN 'No'
      ELSE TRIM(returnFlag)
    END AS return_flag
  FROM source.source_sheet_sheet_09lbl9u5
)
SELECT *
FROM normalized;

CREATE INDEX IF NOT EXISTS idx_transactions_clean_txn_id ON transactions_clean(transaction_id);
CREATE INDEX IF NOT EXISTS idx_transactions_clean_business_date ON transactions_clean(business_date);
CREATE INDEX IF NOT EXISTS idx_transactions_clean_store_id ON transactions_clean(store_id);
CREATE INDEX IF NOT EXISTS idx_transactions_clean_item_sku ON transactions_clean(item_sku);

DROP TABLE IF EXISTS items_clean;
CREATE TABLE items_clean AS
WITH normalized AS (
  SELECT
    UPPER(TRIM(itemSku)) AS item_sku,
    TRIM(itemDescription) AS item_description,
    CASE LOWER(TRIM(category))
      WHEN 'beverages' THEN 'Beverages'
      WHEN 'snacks' THEN 'Snacks'
      WHEN 'produce' THEN 'Produce'
      WHEN 'dairy' THEN 'Dairy'
      WHEN 'bakery' THEN 'Bakery'
      WHEN 'cleaning' THEN 'Cleaning'
      WHEN 'alcohol' THEN 'Alcohol'
      ELSE TRIM(category)
    END AS category,
    TRIM(subcategory) AS subcategory,
    TRIM(brand) AS brand,
    TRIM(packSize) AS pack_size,
    UPPER(TRIM(unitOfMeasure)) AS unit_of_measure,
    CASE LOWER(TRIM(isPrivateLabel))
      WHEN 'yes' THEN 'Yes'
      WHEN 'y' THEN 'Yes'
      WHEN 'true' THEN 'Yes'
      WHEN '1' THEN 'Yes'
      WHEN 'no' THEN 'No'
      WHEN 'n' THEN 'No'
      WHEN 'false' THEN 'No'
      WHEN '0' THEN 'No'
      ELSE TRIM(isPrivateLabel)
    END AS is_private_label,
    CASE
      WHEN NULLIF(TRIM(discontinuedDate), '') IS NULL THEN NULL
      WHEN date(TRIM(discontinuedDate)) IS NOT NULL THEN date(TRIM(discontinuedDate))
      ELSE TRIM(discontinuedDate)
    END AS discontinued_date,
    CAST(baseVatRate AS NUMERIC) AS base_vat_rate,
    CAST(costExVat AS NUMERIC) AS cost_ex_vat,
    CASE
      WHEN NULLIF(TRIM(CAST(shelfLifeDays AS TEXT)), '') IS NULL THEN NULL
      ELSE CAST(shelfLifeDays AS INTEGER)
    END AS shelf_life_days,
    CASE LOWER(TRIM(isAgeRestricted))
      WHEN 'yes' THEN 'Yes'
      WHEN 'y' THEN 'Yes'
      WHEN 'true' THEN 'Yes'
      WHEN '1' THEN 'Yes'
      WHEN 'no' THEN 'No'
      WHEN 'n' THEN 'No'
      WHEN 'false' THEN 'No'
      WHEN '0' THEN 'No'
      ELSE TRIM(isAgeRestricted)
    END AS is_age_restricted,
    CASE LOWER(TRIM(storageType))
      WHEN 'ambient' THEN 'Ambient'
      WHEN 'chilled' THEN 'Chilled'
      ELSE TRIM(storageType)
    END AS storage_type,
    TRIM(countryOfOrigin) AS country_of_origin,
    UPPER(TRIM(supplierId)) AS supplier_id,
    TRIM(supplierName) AS supplier_name
  FROM source.source_sheet_sheet_t4y641kl
)
SELECT *
FROM normalized;

CREATE INDEX IF NOT EXISTS idx_items_clean_item_sku ON items_clean(item_sku);
CREATE INDEX IF NOT EXISTS idx_items_clean_category ON items_clean(category);
CREATE INDEX IF NOT EXISTS idx_items_clean_supplier_id ON items_clean(supplier_id);

DROP TABLE IF EXISTS stores_clean;
CREATE TABLE stores_clean AS
WITH normalized AS (
  SELECT
    UPPER(TRIM(storeId)) AS store_id,
    TRIM(storeName) AS store_name,
    CASE LOWER(TRIM(format))
      WHEN 'hypermarket' THEN 'Hypermarket'
      WHEN 'supermarket' THEN 'Supermarket'
      WHEN 'convenience' THEN 'Convenience'
      WHEN 'discount' THEN 'Discount'
      WHEN 'cash & carry' THEN 'Cash & Carry'
      ELSE TRIM(format)
    END AS store_format,
    CASE LOWER(TRIM(region))
      WHEN 'north' THEN 'North'
      WHEN 'south' THEN 'South'
      WHEN 'west' THEN 'West'
      WHEN 'midwest' THEN 'Midwest'
      WHEN 'northeast' THEN 'Northeast'
      ELSE TRIM(region)
    END AS region,
    TRIM(city) AS city,
    UPPER(TRIM(state)) AS state,
    CAST(squareMeters AS INTEGER) AS square_meters,
    TRIM(cluster) AS cluster
  FROM source.source_sheet_sheet_x2wq8yhk
)
SELECT *
FROM normalized;

CREATE INDEX IF NOT EXISTS idx_stores_clean_store_id ON stores_clean(store_id);
CREATE INDEX IF NOT EXISTS idx_stores_clean_region ON stores_clean(region);

DROP TABLE IF EXISTS business_dates;
CREATE TABLE business_dates AS
SELECT DISTINCT
  business_date
FROM transactions_clean
WHERE business_date IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_business_dates_business_date ON business_dates(business_date);

DROP TABLE IF EXISTS sku_daily_metrics;
CREATE TABLE sku_daily_metrics AS
SELECT
  business_date,
  item_sku,
  SUM(CASE WHEN LOWER(COALESCE(return_flag, '')) IN ('yes', 'y', 'true', '1') THEN 0 ELSE units END) AS units_sold_gross,
  SUM(CASE WHEN LOWER(COALESCE(return_flag, '')) IN ('yes', 'y', 'true', '1') THEN 0 ELSE gross_sales_incl_vat END) AS revenue_incl_vat_gross,
  SUM(CASE WHEN LOWER(COALESCE(return_flag, '')) IN ('yes', 'y', 'true', '1') THEN 0 ELSE cogs_ex_vat END) AS cogs_ex_vat_gross,
  SUM(CASE WHEN LOWER(COALESCE(return_flag, '')) IN ('yes', 'y', 'true', '1') THEN -units ELSE units END) AS units_sold_net,
  SUM(CASE WHEN LOWER(COALESCE(return_flag, '')) IN ('yes', 'y', 'true', '1') THEN -gross_sales_incl_vat ELSE gross_sales_incl_vat END) AS revenue_incl_vat_net,
  SUM(CASE WHEN LOWER(COALESCE(return_flag, '')) IN ('yes', 'y', 'true', '1') THEN -cogs_ex_vat ELSE cogs_ex_vat END) AS cogs_ex_vat_net
FROM transactions_clean
WHERE business_date IS NOT NULL
  AND item_sku IS NOT NULL
GROUP BY business_date, item_sku;

CREATE INDEX IF NOT EXISTS idx_sku_daily_metrics_date_sku ON sku_daily_metrics(business_date, item_sku);
CREATE INDEX IF NOT EXISTS idx_sku_daily_metrics_sku_date ON sku_daily_metrics(item_sku, business_date);