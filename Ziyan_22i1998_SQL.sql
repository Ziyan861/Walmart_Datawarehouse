-- final submitted dwh
drop database if exists walmart_fin;
create DATABASE walmart_fin;
use walmart_fin;
SET FOREIGN_KEY_CHECKS = 0;

DROP VIEW IF EXISTS STORE_QUARTERLY_SALES;
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_store;
DROP TABLE IF EXISTS dim_supplier;
DROP TABLE IF EXISTS dim_date;

SET FOREIGN_KEY_CHECKS = 1;

-- //// dim_date 
CREATE TABLE dim_date (
  date_id  DATE  NOT NULL PRIMARY KEY,
  day_of_week INT NOT NULL,        
  is_weekend INT   NOT NULL,
  week_of_year  INT,
  day_of_month INT,
  month   INT      NOT NULL,
  month_name  VARCHAR(20),
  quarter  INT,
  year    YEAR   NOT NULL,
  season    VARCHAR(10),
  is_holiday   INT  DEFAULT 0) ;

-- // dim_customer 

CREATE TABLE dim_customer (
  customer_id  INT NOT NULL PRIMARY KEY,
  gender   varchar(100),
  age    VARCHAR(20),
  age_group  VARCHAR(20),
  occupation  VARCHAR(100),
  city_category   CHAR(1),
  stay_in_current_city_years VARCHAR(10),
  marital_status  varchar(100)

) ;

--  //dim_supplier 
CREATE TABLE dim_supplier (
  supplier_id    INT  NOT NULL PRIMARY KEY,
  supplier_name  VARCHAR(255)
) ;

-- // dim_store
CREATE TABLE dim_store (
  store_id    INT  NOT NULL PRIMARY KEY,
  store_name     VARCHAR(255),
  region         VARCHAR(100)
 
  ) ;
  

-- //dim_product 
CREATE TABLE dim_product (
  product_id  VARCHAR(32) NOT NULL PRIMARY KEY,
  product_category  VARCHAR(100),
  unit_price  DECIMAL(10,2),
  store_id    INT,
  supplier_id  INT,
  product_name  VARCHAR(255)

);
ALTER TABLE dim_product
DROP COLUMN product_name,
DROP COLUMN store_id,
DROP COLUMN supplier_id;


-- // fact_sales
CREATE TABLE fact_sales (
  order_id  INT NOT NULL PRIMARY KEY,
  customer_id    INT   NOT NULL,
  product_id  VARCHAR(32) NOT NULL,
  store_id  INT,
  supplier_id  INT,
  date_id  DATE NOT NULL,
  quantity  INT NOT NULL,
  unit_price  DECIMAL(10,2) NOT NULL,    --  product price at the time of sale
  total_amount  DECIMAL(12,2) NOT NULL,    -- quantity * unit_price
  timestampp    TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_fact_customer FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id) ON DELETE RESTRICT ON UPDATE CASCADE,
  CONSTRAINT fk_fact_product FOREIGN KEY (product_id) REFERENCES dim_product(product_id) ON DELETE RESTRICT ON UPDATE CASCADE,
  CONSTRAINT fk_fact_store FOREIGN KEY (store_id) REFERENCES dim_store(store_id) ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT fk_fact_supplier FOREIGN KEY (supplier_id) REFERENCES dim_supplier(supplier_id) ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT fk_fact_date FOREIGN KEY (date_id) REFERENCES dim_date(date_id) ON DELETE RESTRICT ON UPDATE CASCADE
) ;

-- view (
CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT 
  s.store_id,
  s.store_name,
  d.year,
  d.quarter,
  SUM(f.total_amount) AS total_sales,
  SUM(f.quantity) AS total_qty
FROM fact_sales f
JOIN dim_store s ON f.store_id = s.store_id
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY s.store_id, s.store_name, d.year, d.quarter
ORDER BY s.store_name, d.year, d.quarter;
DESCRIBE dim_customer;
DROP TABLE fact_sales;
CREATE TABLE fact_sales (
    order_id INT PRIMARY KEY,
    customer_id INT,
    product_id VARCHAR(32),
    date_id DATE,
    quantity INT
);
CREATE TABLE fact_sales_enriched (
    order_id INT,
    customer_id INT,
    product_id VARCHAR(32),
    date_id DATE,
    quantity INT,
    unit_price DECIMAL(10,2),
    store_id INT,
    supplier_id INT,
    total_amount DECIMAL(10,2)
);
describe table dim_store;
table fact_sales_enriched;
table dim_store;

-- OLAPqueries
-- Q1: Top Revenue-Generating Products on Weekdays and Weekends with Monthly Drill-Down
SELECT 
    d.year,
    d.month,
    d.month_name,
    d.is_weekend,
    CASE WHEN d.is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    f.product_id,
    p.product_category,
    SUM(f.total_amount) AS total_revenue,
    SUM(f.quantity) AS total_quantity
FROM fact_sales_enriched f
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_product p ON f.product_id = p.product_id
WHERE d.year = 2017
GROUP BY d.year, d.month, d.month_name, d.is_weekend, f.product_id, p.product_category
ORDER BY d.month, d.is_weekend, total_revenue DESC
LIMIT 60;

-- Q2: Customer Demographics by Purchase Amount with City Category Breakdown
SELECT 
    c.gender,
    c.age_group,
    c.city_category,
    SUM(f.total_amount) AS total_purchase_amount,
    COUNT(DISTINCT f.customer_id) AS customer_count,
    AVG(f.total_amount) AS avg_purchase_amount
FROM fact_sales_enriched f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.gender, c.age_group, c.city_category
ORDER BY total_purchase_amount DESC;

-- Q3: Product Category Sales by Occupation
SELECT 
    c.occupation,
    p.product_category,
    SUM(f.total_amount) AS total_sales,
    SUM(f.quantity) AS total_quantity,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM fact_sales_enriched f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY c.occupation, p.product_category
ORDER BY c.occupation, total_sales DESC;

-- Q4: Total Purchases by Gender and Age Group with Quarterly Trend
SELECT 
    d.year,
    d.quarter,
    c.gender,
    c.age_group,
    SUM(f.total_amount) AS total_purchase_amount,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM fact_sales_enriched f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.year = 2017
GROUP BY d.year, d.quarter, c.gender, c.age_group
ORDER BY d.quarter, c.gender, c.age_group;

-- Q5: Top Occupations by Product Category Sales 
SELECT 
    product_category,
    occupation,
    total_sales,
    sales_rank
FROM (
    SELECT 
        p.product_category,
        c.occupation,
        SUM(f.total_amount) AS total_sales,
        RANK() OVER (PARTITION BY p.product_category ORDER BY SUM(f.total_amount) DESC) AS sales_rank
    FROM fact_sales_enriched f
    JOIN dim_customer c ON f.customer_id = c.customer_id
    JOIN dim_product p ON f.product_id = p.product_id
    GROUP BY p.product_category, c.occupation
) ranked
WHERE sales_rank <= 5
ORDER BY product_category, sales_rank;


-- Q6: City Category Performance by Marital Status - Last 6 months of 2017
SELECT 
    d.year,
    d.month,
    d.month_name,
    c.city_category,
    c.marital_status,
    SUM(f.total_amount) AS total_purchase_amount,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM fact_sales_enriched f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.year = 2017 AND d.month >= 7
GROUP BY d.year, d.month, d.month_name, c.city_category, c.marital_status
ORDER BY d.year, d.month, c.city_category, c.marital_status;

-- Q7: Average Purchase Amount by Stay Duration and Gender
SELECT 
    c.stay_in_current_city_years,
    c.gender,
    AVG(f.total_amount) AS avg_purchase_amount,
    SUM(f.total_amount) AS total_purchase_amount,
    COUNT(DISTINCT f.customer_id) AS customer_count
FROM fact_sales_enriched f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.stay_in_current_city_years, c.gender
ORDER BY c.stay_in_current_city_years, c.gender;

-- Q8: Top 5 Revenue-Generating Cities by Product Category
SELECT 
    product_category,
    city_category,
    total_revenue,
    city_rank
FROM (
    SELECT 
        p.product_category,
        c.city_category,
        SUM(f.total_amount) AS total_revenue,
        RANK() OVER (PARTITION BY p.product_category ORDER BY SUM(f.total_amount) DESC) AS city_rank
    FROM fact_sales_enriched f
    JOIN dim_customer c ON f.customer_id = c.customer_id
    JOIN dim_product p ON f.product_id = p.product_id
    GROUP BY p.product_category, c.city_category
) ranked
WHERE city_rank <= 5
ORDER BY product_category, city_rank;

-- Q9: Monthly Sales Growth by Product Category
SELECT 
    d.year,
    d.month,
    d.month_name,
    p.product_category,
    SUM(f.total_amount) AS current_month_sales,
    LAG(SUM(f.total_amount)) OVER (PARTITION BY p.product_category ORDER BY d.year, d.month) AS previous_month_sales,
    ROUND(((SUM(f.total_amount) - LAG(SUM(f.total_amount)) OVER (PARTITION BY p.product_category ORDER BY d.year, d.month)) / 
           LAG(SUM(f.total_amount)) OVER (PARTITION BY p.product_category ORDER BY d.year, d.month)) * 100, 2) AS growth_percentage
FROM fact_sales_enriched f
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_product p ON f.product_id = p.product_id
WHERE d.year = 2017
GROUP BY d.year, d.month, d.month_name, p.product_category
ORDER BY p.product_category, d.month;

-- Q10: Weekend vs. Weekday Sales by Age Group
SELECT 
    c.age_group,
    CASE WHEN d.is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    SUM(f.total_amount) AS total_sales,
    COUNT(DISTINCT f.order_id) AS total_orders,
    AVG(f.total_amount) AS avg_order_value
FROM fact_sales_enriched f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.year = 2017
GROUP BY c.age_group, d.is_weekend
ORDER BY c.age_group, d.is_weekend;

-- Q11: Top 5 Products by Revenue - Weekdays vs Weekends Monthly Drill-Down
SELECT 
    year,month,day_type,product_id,
    product_category,
    revenue,
    product_rank
FROM (
    SELECT 
        d.year,
        d.month,
        CASE WHEN d.is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
        f.product_id,
        p.product_category,
        SUM(f.total_amount) AS revenue,
        RANK() OVER (PARTITION BY d.year, d.month, d.is_weekend ORDER BY SUM(f.total_amount) DESC) AS product_rank
    FROM fact_sales_enriched f
    JOIN dim_date d ON f.date_id = d.date_id
    JOIN dim_product p ON f.product_id = p.product_id
    WHERE d.year = 2017
    GROUP BY d.year, d.month, d.is_weekend, f.product_id, p.product_category
) ranked
WHERE product_rank <= 5
ORDER BY month, day_type, product_rank;

-- Q12: Trend Analysis of Store Revenue Growth Rate Quarterly for 2017
SELECT 
    s.store_id,
    s.store_name,
    d.quarter,
    SUM(f.total_amount) AS quarterly_revenue,
    LAG(SUM(f.total_amount)) OVER (PARTITION BY s.store_id ORDER BY d.quarter) AS previous_quarter_revenue,
    ROUND(((SUM(f.total_amount) - LAG(SUM(f.total_amount)) OVER (PARTITION BY s.store_id ORDER BY d.quarter)) / 
           LAG(SUM(f.total_amount)) OVER (PARTITION BY s.store_id ORDER BY d.quarter)) * 100, 2) AS growth_rate
FROM fact_sales_enriched f
JOIN dim_store s ON f.store_id = s.store_id
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.year = 2017
GROUP BY s.store_id, s.store_name, d.quarter
ORDER BY s.store_id, d.quarter;

-- Q13: Detailed Supplier Sales Contribution by Store and Product
SELECT 
    s.store_id,
    s.store_name,
    sup.supplier_id,
    sup.supplier_name,
    f.product_id,
    p.product_category,
    SUM(f.total_amount) AS total_sales,
    SUM(f.quantity) AS total_quantity
FROM fact_sales_enriched f
JOIN dim_store s ON f.store_id = s.store_id
JOIN dim_supplier sup ON f.supplier_id = sup.supplier_id
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY s.store_id, s.store_name, sup.supplier_id, sup.supplier_name, f.product_id, p.product_category
ORDER BY s.store_name, sup.supplier_name, total_sales DESC;

-- Q14: Seasonal Analysis of Product Sales Using Dynamic Drill-Down
SELECT 
    d.season,
    p.product_id,
    p.product_category,
    SUM(f.total_amount) AS total_sales,
    SUM(f.quantity) AS total_quantity,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM fact_sales_enriched f
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY d.season, p.product_id, p.product_category
ORDER BY d.season, total_sales DESC;

-- Q15: Store-Wise and Supplier-Wise Monthly Revenue Volatility
SELECT 
    s.store_id,
    s.store_name,
    sup.supplier_id,
    sup.supplier_name,
    d.year,
    d.month,
    SUM(f.total_amount) AS monthly_revenue,
    LAG(SUM(f.total_amount)) OVER (PARTITION BY s.store_id, sup.supplier_id ORDER BY d.year, d.month) AS previous_month_revenue,
    ROUND(((SUM(f.total_amount) - LAG(SUM(f.total_amount)) OVER (PARTITION BY s.store_id, sup.supplier_id ORDER BY d.year, d.month)) / 
           NULLIF(LAG(SUM(f.total_amount)) OVER (PARTITION BY s.store_id, sup.supplier_id ORDER BY d.year, d.month), 0)) * 100, 2) AS volatility_percentage
FROM fact_sales_enriched f
JOIN dim_store s ON f.store_id = s.store_id
JOIN dim_supplier sup ON f.supplier_id = sup.supplier_id
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY s.store_id, s.store_name, sup.supplier_id, sup.supplier_name, d.year, d.month
ORDER BY s.store_id, sup.supplier_id, d.year, d.month;

-- Q16: Top 5 Products Purchased Together (Product Affinity Analysis)
SELECT 
    f1.product_id AS product1,
    f2.product_id AS product2,
    COUNT(DISTINCT f1.customer_id) AS times_bought_together,
    SUM(f1.total_amount + f2.total_amount) AS combined_revenue
FROM fact_sales_enriched f1
JOIN fact_sales_enriched f2 
    ON f1.customer_id = f2.customer_id 
    AND f1.date_id = f2.date_id 
    AND f1.product_id < f2.product_id
GROUP BY f1.product_id, f2.product_id
ORDER BY times_bought_together DESC
LIMIT 5;

-- Q17: Yearly Revenue Trends by Store, Supplier, and Product with ROLLUP
SELECT 
    d.year,
    s.store_name,
    sup.supplier_name,
    p.product_id,
    SUM(f.total_amount) AS total_revenue,
    SUM(f.quantity) AS total_quantity
FROM fact_sales_enriched f
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_store s ON f.store_id = s.store_id
JOIN dim_supplier sup ON f.supplier_id = sup.supplier_id
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY d.year, s.store_name, sup.supplier_name, p.product_id WITH ROLLUP
ORDER BY d.year, s.store_name, sup.supplier_name, p.product_id;

-- Q18: Revenue and Volume-Based Sales Analysis for H1 and H2
SELECT 
    p.product_id,
    p.product_category,
    SUM(CASE WHEN d.month BETWEEN 1 AND 6 THEN f.total_amount ELSE 0 END) AS h1_revenue,
    SUM(CASE WHEN d.month BETWEEN 7 AND 12 THEN f.total_amount ELSE 0 END) AS h2_revenue,
    SUM(CASE WHEN d.month BETWEEN 1 AND 6 THEN f.quantity ELSE 0 END) AS h1_quantity,
    SUM(CASE WHEN d.month BETWEEN 7 AND 12 THEN f.quantity ELSE 0 END) AS h2_quantity,
    SUM(f.total_amount) AS yearly_revenue,
    SUM(f.quantity) AS yearly_quantity
FROM fact_sales_enriched f
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_product p ON f.product_id = p.product_id
WHERE d.year = 2017
GROUP BY p.product_id, p.product_category
ORDER BY yearly_revenue DESC;

-- Q19: Identify High Revenue Spikes in Product Sales (Outliers)
WITH daily_product_sales AS (
    SELECT 
        f.product_id,
        p.product_category,
        f.date_id,
        SUM(f.total_amount) AS daily_sales
    FROM fact_sales_enriched f
    JOIN dim_product p ON f.product_id = p.product_id
    GROUP BY f.product_id, p.product_category, f.date_id
),
product_avg AS (
    SELECT 
        product_id,
        product_category,
        AVG(daily_sales) AS avg_daily_sales,
        STDDEV(daily_sales) AS stddev_sales
    FROM daily_product_sales
    GROUP BY product_id, product_category
)
SELECT 
    dps.product_id,
    dps.product_category,
    dps.date_id,
    dps.daily_sales,
    pa.avg_daily_sales,
    ROUND(dps.daily_sales / pa.avg_daily_sales, 2) AS sales_multiplier,
    CASE 
        WHEN dps.daily_sales > (pa.avg_daily_sales * 2) THEN 'SPIKE/OUTLIER'
        ELSE 'NORMAL'
    END AS anomaly_flag
FROM daily_product_sales dps
JOIN product_avg pa ON dps.product_id = pa.product_id
WHERE dps.daily_sales > (pa.avg_daily_sales * 2)
ORDER BY sales_multiplier DESC
LIMIT 50;

-- Q20: Recreate View STORE_QUARTERLY_SALES for Optimized Sales Analysis
DROP VIEW IF EXISTS STORE_QUARTERLY_SALES;
CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT 
    s.store_id,
    s.store_name,
    d.year,
    d.quarter,
    SUM(f.total_amount) AS total_sales,
    SUM(f.quantity) AS total_qty
FROM fact_sales_enriched f
JOIN dim_store s ON f.store_id = s.store_id
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY s.store_id, s.store_name, d.year, d.quarter
ORDER BY s.store_name, d.year, d.quarter;

-- Now querying it
SELECT * FROM STORE_QUARTERLY_SALES
ORDER BY store_name, year, quarter
LIMIT 20;






