USE SalesDataWarehouse;
GO
-- Dimension Tables; Customers,Products

CREATE VIEW gold.dim_customers AS
SELECT
ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key , --surrogate key
c.cst_id AS customer_id,
c.cst_key AS customer_number,
c.cst_firstname AS first_name,
c.cst_lastname AS last_name,
l.cntry AS country,
c.cst_marital_status AS martial_status,
CASE WHEN c.cst_gndr !='Unknown' THEN  c.cst_gndr -- CRM is the master for gender info
	 ELSE COALESCE(e.gen,'Unknown')
END AS gender,
e.bdate AS birthdate,
c.cst_create_date AS create_date
FROM silver.crm_cust_info c
LEFT JOIN silver.erp_cust_az12 e
ON c.cst_key = e.cid
LEFT JOIN silver.erp_loc_a101 l ON 
c.cst_key=l.cid;

CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER()OVER(ORDER BY prd_id) AS product_key,
	p.prd_id AS product_id,
	p.prd_key AS product_number,
	p.prd_nm AS product_name,
	p.cat_id AS category_id,
	ca.cat AS category,
	ca.subcat AS subcategory,
	p.prd_line AS product_line,
	ca.maintenance,
	p.prd_cost AS product_cost,
	p.prd_start_dt AS start_date
FROM silver.crm_prd_info p
LEFT JOIN silver.erp_px_cat_g1v2 ca
ON p.cat_id=ca.id
WHERE p.prd_end_dt IS NULL ; -- Filtering out historical data (end_date is null >> current data)

-- FACT table: Sales, Connecting the fact to dimensions by surrogate keys
CREATE VIEW gold.fact_sales AS
SELECT 
s.sls_ord_num AS order_number,
pr.product_key,
cus.customer_key,
s.sls_order_dt as order_date,
s.sls_ship_dt AS ship_date,
s.sls_due_dt AS due_date,
s.sls_sales AS sales_amount,
s.sls_quantity AS quantity,
s.sls_price AS price
FROM silver.crm_sales_details s
LEFT JOIN gold.dim_products pr
ON s.sls_prd_key=pr.product_number
LEFT JOIN gold.dim_customers cus
ON s.sls_cust_id=cus.customer_id
;


