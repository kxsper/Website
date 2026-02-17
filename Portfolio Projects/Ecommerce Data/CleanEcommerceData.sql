-- PHASE 0: CREATING THE STAGING TABLES 

CREATE TABLE IF NOT EXISTS olist_orders_dataset (
    order_id                        TEXT,
    customer_id                     TEXT,
    order_status                    TEXT,
    order_purchase_timestamp        TEXT,
    order_approved_at               TEXT,
    order_delivered_carrier_date    TEXT,
    order_delivered_customer_date   TEXT,
    order_estimated_delivery_date   TEXT
);

CREATE TABLE IF NOT EXISTS olist_order_items_dataset (
    order_id            TEXT,
    order_item_id       TEXT,
    product_id          TEXT,
    seller_id           TEXT,
    shipping_limit_date TEXT,
    price               TEXT,
    freight_value       TEXT
);

CREATE TABLE IF NOT EXISTS olist_order_payments_dataset (
    order_id              TEXT,
    payment_sequential    TEXT,
    payment_type          TEXT,
    payment_installments  TEXT,
    payment_value         TEXT
);

CREATE TABLE IF NOT EXISTS olist_order_reviews_dataset (
    review_id                TEXT,
    order_id                 TEXT,
    review_score             TEXT,
    review_comment_title     TEXT,
    review_comment_message   TEXT,
    review_creation_date     TEXT,
    review_answer_timestamp  TEXT
);

CREATE TABLE IF NOT EXISTS olist_customers_dataset (
    customer_id              TEXT,
    customer_unique_id       TEXT,
    customer_zip_code_prefix TEXT,
    customer_city            TEXT,
    customer_state           TEXT
);

CREATE TABLE IF NOT EXISTS olist_products_dataset (
    product_id                   TEXT,
    product_category_name        TEXT,
    product_name_lenght          TEXT,   -- source typo preserved
    product_description_lenght   TEXT,   -- source typo preserved
    product_photos_qty           TEXT,
    product_weight_g             TEXT,
    product_length_cm            TEXT,
    product_height_cm            TEXT,
    product_width_cm             TEXT
);

CREATE TABLE IF NOT EXISTS olist_sellers_dataset (
    seller_id                TEXT,
    seller_zip_code_prefix   TEXT,
    seller_city              TEXT,
    seller_state             TEXT
);

CREATE TABLE IF NOT EXISTS olist_geolocation_dataset (
    geolocation_zip_code_prefix  TEXT,
    geolocation_lat              TEXT,
    geolocation_lng              TEXT,
    geolocation_city             TEXT,
    geolocation_state            TEXT
);

CREATE TABLE IF NOT EXISTS product_category_name_translation (
    product_category_name          TEXT,
    product_category_name_english  TEXT
);


-- PHASE 1: DATA PROFILING

-- Row counts across all raw tables
SELECT 'olist_orders_dataset'              AS table_name, COUNT(*) AS row_count FROM olist_orders_dataset         UNION ALL
SELECT 'olist_order_items_dataset',         COUNT(*) FROM olist_order_items_dataset        UNION ALL
SELECT 'olist_order_payments_dataset',      COUNT(*) FROM olist_order_payments_dataset     UNION ALL
SELECT 'olist_order_reviews_dataset',       COUNT(*) FROM olist_order_reviews_dataset      UNION ALL
SELECT 'olist_customers_dataset',           COUNT(*) FROM olist_customers_dataset          UNION ALL
SELECT 'olist_products_dataset',            COUNT(*) FROM olist_products_dataset           UNION ALL
SELECT 'olist_sellers_dataset',             COUNT(*) FROM olist_sellers_dataset            UNION ALL
SELECT 'olist_geolocation_dataset',         COUNT(*) FROM olist_geolocation_dataset        UNION ALL
SELECT 'product_category_name_translation', COUNT(*) FROM product_category_name_translation
ORDER BY row_count DESC;

-- NULL audit on orders
SELECT
    COUNT(*)                                                                     AS total_orders,
    COUNT(*) FILTER (WHERE order_id IS NULL OR order_id = '')                    AS null_order_id,
    COUNT(*) FILTER (WHERE customer_id IS NULL OR customer_id = '')              AS null_customer_id,
    COUNT(*) FILTER (WHERE order_status IS NULL OR order_status = '')            AS null_status,
    COUNT(*) FILTER (WHERE order_purchase_timestamp IS NULL)                     AS null_purchase_ts,
    COUNT(*) FILTER (WHERE order_delivered_customer_date IS NULL)                AS null_delivered
FROM olist_orders_dataset;

-- Order status breakdown
SELECT order_status, COUNT(*) AS cnt
FROM olist_orders_dataset
GROUP BY order_status
ORDER BY cnt DESC;

-- Payment type breakdown
SELECT payment_type, COUNT(*) AS cnt
FROM olist_order_payments_dataset
GROUP BY payment_type
ORDER BY cnt DESC;

-- NULL audit on products
SELECT
    COUNT(*)                                                                               AS total_products,
    COUNT(*) FILTER (WHERE product_category_name IS NULL OR product_category_name = '')   AS null_category,
    COUNT(*) FILTER (WHERE product_weight_g IS NULL)                                      AS null_weight,
    COUNT(*) FILTER (WHERE product_length_cm IS NULL)                                     AS null_length
FROM olist_products_dataset;

-- Geolocation duplicates per zip 
SELECT geolocation_zip_code_prefix, COUNT(*) AS cnt
FROM olist_geolocation_dataset
GROUP BY geolocation_zip_code_prefix
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 10;


-- PHASE 2: DROP OUTPUT TABLES

DROP TABLE IF EXISTS fact_orders;
DROP TABLE IF EXISTS dim_customers;
DROP TABLE IF EXISTS dim_products;


-- PHASE 3: Creating the dim_customers table
-- Using these sources: (olist_customers_dataset, olist_geolocation_dataset)

-- Cleaning applied:
--   • Deduplicating geolocation (avg lat/lng per zip — 1M rows -> 19k)
--   • INITCAP() for proper title casing of city names
--   • REPLACE underscores in city names with spaces
--   • UPPER() state codes, mapped to full state names via CASE
--   • COALESCE coordinates to 0.0 where no geo match found
--   • Flag rows with no matching geolocation record

CREATE TABLE dim_customers AS

WITH geo_deduped AS (
    SELECT
        geolocation_zip_code_prefix,
        ROUND(AVG(geolocation_lat::NUMERIC), 6) AS latitude,
        ROUND(AVG(geolocation_lng::NUMERIC), 6) AS longitude
    FROM olist_geolocation_dataset
    GROUP BY geolocation_zip_code_prefix
),

customers_cleaned AS (
    SELECT
        c.customer_id,
        c.customer_unique_id,
        INITCAP(TRIM(REPLACE(c.customer_city, '_', ' '))) AS customer_city,
        UPPER(TRIM(c.customer_state))                      AS customer_state,

        CASE UPPER(TRIM(c.customer_state))
            WHEN 'AC' THEN 'Acre'                WHEN 'AL' THEN 'Alagoas'
            WHEN 'AP' THEN 'Amapa'               WHEN 'AM' THEN 'Amazonas'
            WHEN 'BA' THEN 'Bahia'               WHEN 'CE' THEN 'Ceara'
            WHEN 'DF' THEN 'Distrito Federal'    WHEN 'ES' THEN 'Espirito Santo'
            WHEN 'GO' THEN 'Goias'               WHEN 'MA' THEN 'Maranhao'
            WHEN 'MT' THEN 'Mato Grosso'         WHEN 'MS' THEN 'Mato Grosso do Sul'
            WHEN 'MG' THEN 'Minas Gerais'        WHEN 'PA' THEN 'Para'
            WHEN 'PB' THEN 'Paraiba'             WHEN 'PR' THEN 'Parana'
            WHEN 'PE' THEN 'Pernambuco'          WHEN 'PI' THEN 'Piaui'
            WHEN 'RJ' THEN 'Rio de Janeiro'      WHEN 'RN' THEN 'Rio Grande do Norte'
            WHEN 'RS' THEN 'Rio Grande do Sul'   WHEN 'RO' THEN 'Rondonia'
            WHEN 'RR' THEN 'Roraima'             WHEN 'SC' THEN 'Santa Catarina'
            WHEN 'SP' THEN 'Sao Paulo'           WHEN 'SE' THEN 'Sergipe'
            WHEN 'TO' THEN 'Tocantins'
            ELSE 'Unknown'
        END AS customer_state_full,

        c.customer_zip_code_prefix

    FROM olist_customers_dataset c
)

SELECT
    cc.customer_id,
    cc.customer_unique_id,
    cc.customer_city,
    cc.customer_state,
    cc.customer_state_full,
    cc.customer_zip_code_prefix,
    COALESCE(g.latitude,  0.0) AS latitude,
    COALESCE(g.longitude, 0.0) AS longitude,
    CASE WHEN g.geolocation_zip_code_prefix IS NOT NULL THEN 1 ELSE 0 END AS has_geolocation

FROM customers_cleaned cc
LEFT JOIN geo_deduped g
    ON cc.customer_zip_code_prefix = g.geolocation_zip_code_prefix;

-- Validation
SELECT
    COUNT(*)                        AS total_customers,
    SUM(has_geolocation)            AS with_geo,
    COUNT(*) - SUM(has_geolocation) AS without_geo,
    COUNT(DISTINCT customer_state)  AS distinct_states
FROM dim_customers;



-- PHASE 4: Creating the dim_products table
-- Using these sources: (olist_products_dataset, product_category_name_translation)

-- Cleaning applied:
--   • LEFT JOIN English translation, COALESCE NULLs to 'uncategorised'
--   • NULLIF handles empty strings before COALESCE
--   • REPLACE underscores with spaces in category names
--   • PERCENTILE_CONT(0.5) = true median for NULL dimension imputation
--     (this is more accurate than AVG for skewed product dimension distributions)
--   • Derive product_volume_cm3 from (length x height x width)
--   • Bin into size and weight tiers using CASE
--   • Flag missing photos and missing descriptions
--   • Note: the source columns have a typo ("lenght")

CREATE TABLE dim_products AS

WITH dimension_medians AS (
    SELECT
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY product_weight_g::NUMERIC)  AS median_weight,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY product_length_cm::NUMERIC) AS median_length,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY product_height_cm::NUMERIC) AS median_height,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY product_width_cm::NUMERIC)  AS median_width
    FROM olist_products_dataset
    WHERE product_weight_g  IS NOT NULL
      AND product_length_cm IS NOT NULL
      AND product_height_cm IS NOT NULL
      AND product_width_cm  IS NOT NULL
),

products_base AS (
    SELECT
        p.product_id,

        TRIM(REPLACE(
            COALESCE(NULLIF(t.product_category_name_english, ''), 'uncategorised'),
        '_', ' ')) AS product_category_english,

        TRIM(REPLACE(
            COALESCE(NULLIF(p.product_category_name, ''), 'uncategorised'),
        '_', ' ')) AS product_category_portuguese,

        COALESCE(p.product_weight_g::NUMERIC,  m.median_weight) AS product_weight_g,
        COALESCE(p.product_length_cm::NUMERIC, m.median_length) AS product_length_cm,
        COALESCE(p.product_height_cm::NUMERIC, m.median_height) AS product_height_cm,
        COALESCE(p.product_width_cm::NUMERIC,  m.median_width)  AS product_width_cm,

        COALESCE(p.product_photos_qty::INTEGER,         0) AS product_photos_qty,
        COALESCE(p.product_description_lenght::INTEGER, 0) AS product_description_length,
        COALESCE(p.product_name_lenght::INTEGER,        0) AS product_name_length

    FROM olist_products_dataset p
    CROSS JOIN dimension_medians m
    LEFT JOIN product_category_name_translation t
        ON p.product_category_name = t.product_category_name
)

SELECT
    product_id,
    product_category_english,
    product_category_portuguese,
    ROUND(product_weight_g::NUMERIC,  2) AS product_weight_g,
    ROUND(product_length_cm::NUMERIC, 2) AS product_length_cm,
    ROUND(product_height_cm::NUMERIC, 2) AS product_height_cm,
    ROUND(product_width_cm::NUMERIC,  2) AS product_width_cm,
    product_photos_qty,
    product_description_length,
    product_name_length,

    ROUND((product_length_cm * product_height_cm * product_width_cm)::NUMERIC, 2) AS product_volume_cm3,

    CASE
        WHEN (product_length_cm * product_height_cm * product_width_cm) <  1000 THEN 'Extra Small'
        WHEN (product_length_cm * product_height_cm * product_width_cm) <  5000 THEN 'Small'
        WHEN (product_length_cm * product_height_cm * product_width_cm) < 15000 THEN 'Medium'
        WHEN (product_length_cm * product_height_cm * product_width_cm) < 50000 THEN 'Large'
        ELSE 'Extra Large'
    END AS product_size_tier,

    CASE
        WHEN product_weight_g <  200  THEN 'Light'
        WHEN product_weight_g < 1000  THEN 'Medium'
        WHEN product_weight_g < 5000  THEN 'Heavy'
        ELSE 'Very Heavy'
    END AS product_weight_tier,

    CASE WHEN product_photos_qty         = 0 THEN 1 ELSE 0 END AS missing_photos_flag,
    CASE WHEN product_description_length = 0 THEN 1 ELSE 0 END AS missing_description_flag

FROM products_base;

-- Validation
SELECT
    product_size_tier,
    COUNT(*)                        AS products_in_tier,
    SUM(missing_photos_flag)        AS missing_photos,
    SUM(missing_description_flag)   AS missing_descriptions
FROM dim_products
GROUP BY product_size_tier
ORDER BY products_in_tier DESC;


-- PHASE 5: Creating the fact_orders table
-- Using these sources: (olist_order_items_dataset, olist_orders_dataset,
--            			 olist_order_payments_dataset, olist_order_reviews_dataset,
--            			 olist_sellers_dataset)

-- Cleaning applied:
--   • Timestamps cast from TEXT to TIMESTAMP for proper arithmetic
--   • EXTRACT() for all date parts, no SUBSTR hacks needed
--   • TO_CHAR() for day name, no 7 branch CASE needed
--   • EPOCH based delivery durations, most precise method in Postgres
--   • FILTER clause for conditional aggregation, this is cleaner than CASE WHEN
--   • NTILE(4) window function for seller revenue quartiles, one line,
--     replaces all the correlated subquery workarounds from SQLite
--   • INITCAP() for seller city name casing
--   • ROW_NUMBER() window function to find primary payment method per order

CREATE TABLE fact_orders AS

-- CTE 1: clean and parse all order timestamps in one place
WITH orders_clean AS (
    SELECT
        order_id,
        customer_id,
        TRIM(order_status) AS order_status,

        CASE TRIM(order_status)
            WHEN 'delivered'   THEN 'Completed'
            WHEN 'shipped'     THEN 'In Transit'
            WHEN 'processing'  THEN 'Processing'
            WHEN 'approved'    THEN 'Processing'
            WHEN 'invoiced'    THEN 'Processing'
            WHEN 'canceled'    THEN 'Cancelled'
            WHEN 'unavailable' THEN 'Cancelled'
            WHEN 'created'     THEN 'Processing'
            ELSE 'Unknown'
        END AS order_status_group,

        -- Cast text to proper TIMESTAMP, this unlocks all native date functions
        order_purchase_timestamp::TIMESTAMP       AS purchase_ts,
        order_approved_at::TIMESTAMP              AS approved_ts,
        order_delivered_carrier_date::TIMESTAMP   AS carrier_ts,
        order_delivered_customer_date::TIMESTAMP  AS delivered_ts,
        order_estimated_delivery_date::TIMESTAMP  AS estimated_ts,

        -- Date part extractions
        order_purchase_timestamp::DATE                                       AS purchase_date,
        EXTRACT(YEAR    FROM order_purchase_timestamp::TIMESTAMP)::INTEGER   AS purchase_year,
        EXTRACT(MONTH   FROM order_purchase_timestamp::TIMESTAMP)::INTEGER   AS purchase_month,
        EXTRACT(DAY     FROM order_purchase_timestamp::TIMESTAMP)::INTEGER   AS purchase_day,
        EXTRACT(DOW     FROM order_purchase_timestamp::TIMESTAMP)::INTEGER   AS purchase_day_of_week,

        -- TO_CHAR gives the full day name in one call, so there is no CASE statement needed
        TRIM(TO_CHAR(order_purchase_timestamp::TIMESTAMP, 'Day'))            AS purchase_day_name,

        -- Quarter in one expression — no 12-branch CASE needed
        'Q' || EXTRACT(QUARTER FROM order_purchase_timestamp::TIMESTAMP)::INTEGER AS purchase_quarter,

        -- Delivery durations using EPOCH (seconds / 86400 = days), this is most accurate in Postgres
        CASE
            WHEN order_delivered_customer_date IS NOT NULL
             AND order_purchase_timestamp IS NOT NULL
            THEN ROUND(
                EXTRACT(EPOCH FROM (
                    order_delivered_customer_date::TIMESTAMP - order_purchase_timestamp::TIMESTAMP
                )) / 86400.0, 1
            )
        END AS actual_delivery_days,

        CASE
            WHEN order_estimated_delivery_date IS NOT NULL
             AND order_purchase_timestamp IS NOT NULL
            THEN ROUND(
                EXTRACT(EPOCH FROM (
                    order_estimated_delivery_date::TIMESTAMP - order_purchase_timestamp::TIMESTAMP
                )) / 86400.0, 1
            )
        END AS estimated_delivery_days,

        -- Delta: positive = late, negative = early, NULL = not yet delivered
        CASE
            WHEN order_delivered_customer_date IS NOT NULL
             AND order_estimated_delivery_date IS NOT NULL
            THEN ROUND(
                EXTRACT(EPOCH FROM (
                    order_delivered_customer_date::TIMESTAMP - order_estimated_delivery_date::TIMESTAMP
                )) / 86400.0, 1
            )
        END AS delivery_delta_days,

        -- Data quality flag: delivered before purchased (bad data)
        CASE
            WHEN order_delivered_customer_date IS NOT NULL
             AND order_delivered_customer_date::TIMESTAMP < order_purchase_timestamp::TIMESTAMP
            THEN 1 ELSE 0
        END AS date_logic_error_flag

    FROM olist_orders_dataset
),

-- CTE 2: payments pivoted to order level
-- FILTER clause is cleaner and faster than CASE WHEN ELSE 0 etc
payments_agg AS (
    SELECT
        order_id,
        ROUND(SUM(payment_value::NUMERIC), 2)                                              AS total_payment_value,
        ROUND(SUM(payment_value::NUMERIC) FILTER (WHERE payment_type = 'credit_card'), 2)  AS payment_credit_card,
        ROUND(SUM(payment_value::NUMERIC) FILTER (WHERE payment_type = 'boleto'),      2)  AS payment_boleto,
        ROUND(SUM(payment_value::NUMERIC) FILTER (WHERE payment_type = 'voucher'),     2)  AS payment_voucher,
        ROUND(SUM(payment_value::NUMERIC) FILTER (WHERE payment_type = 'debit_card'),  2)  AS payment_debit_card,
        ROUND(SUM(payment_value::NUMERIC) FILTER (WHERE payment_type = 'not_defined'), 2)  AS payment_not_defined,

        -- Primary payment method: highest spend type for this order
        -- ROW_NUMBER() ranks payment types by spend
        (
            SELECT payment_type FROM (
                SELECT
                    payment_type,
                    ROW_NUMBER() OVER (ORDER BY SUM(payment_value::NUMERIC) DESC) AS rn
                FROM olist_order_payments_dataset ip
                WHERE ip.order_id = p.order_id
                  AND ip.payment_type != 'not_defined'
                GROUP BY payment_type
            ) ranked
            WHERE rn = 1
        ) AS primary_payment_method,

        MAX(payment_installments::INTEGER)                                  AS max_instalments,
        COUNT(*)                                                            AS payment_row_count,
        CASE WHEN COUNT(DISTINCT payment_type) > 1 THEN 1 ELSE 0 END       AS is_split_payment

    FROM olist_order_payments_dataset p
    GROUP BY order_id
),

-- CTE 3: reviews aggregated to order level
reviews_agg AS (
    SELECT
        order_id,
        MAX(review_score::INTEGER)               AS max_review_score,
        MIN(review_score::INTEGER)               AS min_review_score,
        ROUND(AVG(review_score::NUMERIC), 2)     AS avg_review_score,
        COUNT(*)                                 AS review_count,

        CASE
            WHEN AVG(review_score::NUMERIC) >= 4.5 THEN 'Very Positive'
            WHEN AVG(review_score::NUMERIC) >= 3.5 THEN 'Positive'
            WHEN AVG(review_score::NUMERIC) >= 2.5 THEN 'Neutral'
            WHEN AVG(review_score::NUMERIC) >= 1.5 THEN 'Negative'
            ELSE 'Very Negative'
        END AS review_sentiment,

        -- FILTER: cleaner than nested CASE WHEN
        MAX(1) FILTER (
            WHERE review_comment_message IS NOT NULL
              AND TRIM(review_comment_message) != ''
              AND LOWER(TRIM(review_comment_message)) != 'null'
        ) AS has_written_review

    FROM olist_order_reviews_dataset
    GROUP BY order_id
),

-- CTE 4: clean order items with derived financials
order_items_clean AS (
    SELECT
        order_id,
        order_item_id,
        product_id,
        seller_id,
        ROUND(price::NUMERIC,         2) AS item_price,
        ROUND(freight_value::NUMERIC, 2) AS freight_value,
        ROUND(price::NUMERIC + freight_value::NUMERIC, 2) AS item_total,
        CASE
            WHEN price::NUMERIC > 0
            THEN ROUND((freight_value::NUMERIC / price::NUMERIC) * 100, 2)
        END AS freight_pct_of_price
    FROM olist_order_items_dataset
    WHERE order_id IS NOT NULL
      AND NULLIF(TRIM(order_id), '') IS NOT NULL
),

-- CTE 5: clean sellers
sellers_clean AS (
    SELECT
        seller_id,
        INITCAP(TRIM(seller_city))  AS seller_city,
        UPPER(TRIM(seller_state))   AS seller_state,
        seller_zip_code_prefix
    FROM olist_sellers_dataset
),

-- CTE 6: seller level revenue aggregates
seller_revenue AS (
    SELECT
        seller_id,
        ROUND(SUM(item_total), 2)  AS seller_total_revenue,
        COUNT(DISTINCT order_id)   AS seller_total_orders,
        ROUND(AVG(item_price), 2)  AS seller_avg_price
    FROM order_items_clean
    GROUP BY seller_id
),

-- CTE 7: NTILE(4) this is the native Postgres window function
seller_quartile AS (
    SELECT
        seller_id,
        seller_total_revenue,
        seller_total_orders,
        seller_avg_price,
        NTILE(4) OVER (ORDER BY seller_total_revenue) AS revenue_quartile_num
    FROM seller_revenue
)

-- Final join, this is all of the CTEs assembled into the fact table
SELECT
    -- Keys
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    o.customer_id,

    -- Order status
    o.order_status,
    o.order_status_group,

    -- Date dimensions
    o.purchase_date,
    o.purchase_year,
    o.purchase_month,
    o.purchase_day,
    o.purchase_day_of_week,
    o.purchase_day_name,
    o.purchase_quarter,

    -- Raw timestamps (kept for PowerBI time intelligence if needed)
    o.purchase_ts,
    o.approved_ts,
    o.carrier_ts,
    o.delivered_ts,
    o.estimated_ts,

    -- Delivery metrics
    o.actual_delivery_days,
    o.estimated_delivery_days,
    o.delivery_delta_days,

    CASE
        WHEN o.delivery_delta_days IS NULL THEN 'Unknown'
        WHEN o.delivery_delta_days  > 0    THEN 'Late'
        WHEN o.delivery_delta_days  = 0    THEN 'On Time'
        ELSE 'Early'
    END AS delivery_timeliness,

    CASE
        WHEN o.actual_delivery_days IS NULL  THEN 'Not Delivered'
        WHEN o.actual_delivery_days <= 3     THEN 'Express (1-3 days)'
        WHEN o.actual_delivery_days <= 7     THEN 'Fast (4-7 days)'
        WHEN o.actual_delivery_days <= 14    THEN 'Standard (8-14 days)'
        WHEN o.actual_delivery_days <= 30    THEN 'Slow (15-30 days)'
        ELSE 'Very Slow (30+ days)'
    END AS delivery_speed_tier,

    o.date_logic_error_flag,

    -- Financials
    oi.item_price,
    oi.freight_value,
    oi.item_total,
    oi.freight_pct_of_price,

    -- Payments 
    pay.total_payment_value,
    pay.primary_payment_method,
    pay.payment_credit_card,
    pay.payment_boleto,
    pay.payment_voucher,
    pay.payment_debit_card,
    pay.max_instalments,
    pay.is_split_payment,

    -- Reviews
    rev.avg_review_score,
    rev.review_sentiment,
    COALESCE(rev.has_written_review, 0) AS has_written_review,
    rev.review_count,

    -- Seller
    s.seller_city,
    s.seller_state,
    s.seller_zip_code_prefix,

    CASE sq.revenue_quartile_num
        WHEN 1 THEN 'Bottom 25%'
        WHEN 2 THEN 'Lower Mid 25%'
        WHEN 3 THEN 'Upper Mid 25%'
        WHEN 4 THEN 'Top 25%'
    END AS seller_revenue_quartile,

    sq.seller_total_revenue,
    sq.seller_total_orders,
    sq.seller_avg_price

FROM order_items_clean     oi
INNER JOIN orders_clean    o   ON oi.order_id  = o.order_id
LEFT  JOIN payments_agg    pay ON oi.order_id  = pay.order_id
LEFT  JOIN reviews_agg     rev ON oi.order_id  = rev.order_id
LEFT  JOIN sellers_clean   s   ON oi.seller_id = s.seller_id
LEFT  JOIN seller_quartile sq  ON oi.seller_id = sq.seller_id;


-- PHASE 6: VALIDATION

-- fact_orders summary
SELECT
    COUNT(*)                    AS total_rows,
    COUNT(DISTINCT order_id)    AS distinct_orders,
    COUNT(DISTINCT customer_id) AS distinct_customers,
    COUNT(DISTINCT product_id)  AS distinct_products,
    COUNT(DISTINCT seller_id)   AS distinct_sellers,
    ROUND(SUM(item_total), 2)   AS total_gmv,
    ROUND(AVG(avg_review_score), 2)                                  AS overall_avg_review,
    COUNT(*) FILTER (WHERE delivery_timeliness = 'Late')             AS late_deliveries,
    COUNT(*) FILTER (WHERE delivery_timeliness = 'Early')            AS early_deliveries,
    COUNT(*) FILTER (WHERE date_logic_error_flag = 1)                AS date_logic_errors
FROM fact_orders;

-- Orphan check: items in fact with no matching product in dim
SELECT COUNT(*) AS orphaned_product_ids
FROM fact_orders f
LEFT JOIN dim_products p ON f.product_id = p.product_id
WHERE p.product_id IS NULL;

-- Orphan check: items in fact with no matching customer in dim
SELECT COUNT(*) AS orphaned_customer_ids
FROM fact_orders f
LEFT JOIN dim_customers c ON f.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Seller quartile revenue distribution
SELECT
    seller_revenue_quartile,
    COUNT(DISTINCT seller_id) AS sellers,
    ROUND(SUM(item_total), 2) AS revenue
FROM fact_orders
GROUP BY seller_revenue_quartile
ORDER BY revenue DESC;