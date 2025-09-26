
USE SalesDataWarehouse;
GO

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	-----------Cleaning crm_cust_info table--------------
	PRINT'>> Truncating table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT'>> Inserting data into: silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
	)

	SELECT cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,--Trimmed space
	TRIM(cst_lastname)AS cst_lastname,
	-- Mapping abberviations into meaningful values,
	CASE WHEN cst_marital_status= UPPER(TRIM('M'))THEN 'Married'
		 WHEN  cst_marital_status= UPPER(TRIM('S'))THEN 'Single'
		 ELSE 'Unknown'

	END
	,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		 WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
		 ELSE 'Unknown'
	END ,
	-- Selecting the most recent record by date
	cst_create_date FROM ( 
	SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	)t WHERE flag_last = 1 ;



	-----------Cleaning prd_info table-------------
	PRINT'>> Truncating table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT'>> Inserting data into: silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt)

	SELECT 
	prd_id,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, -- Derived cat_id from original prd_key ,
	SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key, -- Derived the rest as prd_key from original prd_key,
	prd_nm,
	ISNULL(prd_cost,0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' THEN 'Other Sales'
		 WHEN 'T' THEN 'Touring'
		 WHEN 'M' THEN  'Mountain'
		 ELSE 'Unknown'
	END AS prd_line,
	CAST (prd_start_dt AS DATE) AS prd_start_dt,
	CAST(DATEADD(DAY,-1,LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt )) AS DATE)AS prd_end_dt -- Calculated the end_date as one day before the next start date
	FROM bronze.crm_prd_info
	;

	-----------Cleaning sales_details table--------------
	PRINT'>> Truncating table: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT'>> Inserting data into: silver.crm_sales_details';

	INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price)

	SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt)!=8 THEN NULL -- Reformatting date to be in the correct form
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
	END AS sls_order_dt ,
	CASE WHEN sls_ship_dt=0 OR LEN(sls_ship_dt) !=8 THEN NULL
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR)AS DATE)
	END AS sls_ship_dt,
	CASE WHEN sls_due_dt=0 OR LEN(sls_due_dt) !=8 THEN NULL
		 ELSE CAST(CAST(sls_due_dt AS VARCHAR)AS DATE)
	END AS sls_due_dt,
	CASE -- Recalculating sales in case value is missing or invalid
		WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE 
		WHEN sls_price IS NULL OR sls_price <=0   THEN sls_sales/NULLIF(sls_quantity,0)
		ELSE sls_price
	END AS sls_price

	FROM bronze.crm_sales_details;

	-----------Cleaning erp_cust table--------------
	PRINT'>> Truncating table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT'>> Inserting data into: silver.erp_cust_az12';

	INSERT INTO silver.erp_cust_az12 (
	cid,
	bdate,
	gen)
	SELECT 
	CASE WHEN cid LIKE 'NAS%'THEN SUBSTRING(cid,4,Len(cid)) -- Removed 'NAS' prefix if exist
		 ELSE cid
	END AS cid,
	CASE WHEN bdate > GETDATE() THEN NULL -- Future date to NULL
		 ELSE bdate
	END AS bdate,
	CASE WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
		 WHEN UPPER(TRIM(gen)) IN('F','FEMALE')THEN 'Female'
		 ELSE 'Unknown'
	END As gen
	FROM bronze.erp_cust_az12;

	-----------Cleaning erp_loc_a101 table--------------
	PRINT'>> Truncating table: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT'>> Inserting data into: silver.erp_loc_a101';

	INSERT into silver.erp_loc_a101(
	cid,
	cntry)
	SELECT 
	REPLACE(cid,'-','') AS cid,
	CASE WHEN TRIM(cntry)='DE' THEN 'Germany' -- Normalize and handle country issues
		 WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
		 WHEN TRIM(cntry)='' OR TRIM(cntry) IS NULL THEN 'Unknown'
		 ELSE TRIM(cntry)
	END AS cntry
	FROM bronze.erp_loc_a101;


	-----------Cleaning erp_px_cat_g1v2 table--------------
	PRINT'>> Truncating table:  silver.erp_px_cat_g1v2';
	TRUNCATE TABLE  silver.erp_px_cat_g1v2;
	PRINT'>> Inserting data into:  silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2 (
	id,
	cat,
	subcat,
	maintenance)

	SELECT 
	id,
	cat,
	subcat,
	maintenance
	FROM bronze.erp_px_cat_g1v2;
END
