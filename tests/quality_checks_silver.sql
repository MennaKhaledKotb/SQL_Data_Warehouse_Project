USE SalesDataWarehouse;
GO

-- Checking For Nulls or Duplicates
SELECT cst_id,COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id IS NULL;

-- Data standardization & consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Check for spaces
SELECT cst_key
FROM bronze.crm_cust_info
WHERE cst_key !=TRIM(cst_key);


-- Checking for Negative numbers or Nulls
SELECT *
FROM silver.crm_prd_info
WHERE prd_cost <0 OR prd_cost IS NULL;

--Checking for Invalid End dates
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


----------- sales_details table--------------

SELECT*
FROM bronze.crm_sales_details;


SELECT *
FROM bronze.crm_sales_details
WHERE sls_prd_key IN(
SELECT prd_key
FROM silver.crm_prd_info )
;

-- Checking date validity 
SELECT NULLIF(sls_order_dt,0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <=0 OR len(sls_order_dt) !=8 OR sls_order_dt >20250925;

SELECT *
FROM  bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt> sls_due_dt;

--Checking sales
SELECT
sls_sales,
sls_quantity,
sls_price
FROM  bronze.crm_sales_details
WHERE sls_sales IS NULL OR sls_sales<=0 OR sls_sales != sls_quantity*sls_price
OR sls_quantity IS NULL OR sls_price IS NULL OR sls_quantity<=0 OR sls_price<=0 
ORDER BY sls_sales,
sls_quantity,
sls_price;

-----------erp_cust table--------------

SELECT *
FROM bronze.erp_cust_az12;

SELECT DISTINCT gen
FROM bronze.erp_cust_az12;


SELECT bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

----------- erp_loc_a101 table--------------

SELECT *
FROM bronze.erp_loc_a101;

SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;


SELECT cntry,
REPLACE(cid,'-','') AS cid,
CASE WHEN TRIM(cntry)='DE' THEN 'Germany'
     WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	 WHEN TRIM(cntry)='' OR TRIM(cntry) IS NULL THEN 'Unknown'
	 ELSE TRIM(cntry)
END AS new_cntry
FROM bronze.erp_loc_a101
Order by cntry;


-----------erp_px_cat_g1v2 table-------------
--Check for unwanted spaces
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance) ;


SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2;
