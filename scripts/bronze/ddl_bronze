
CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN
	DECLARE @start_time DATETIME , @end_time DATETIME,@batch_start DATETIME,@batch_end DATETIME;
	BEGIN TRY
		SET @batch_start= GETDATE();
		PRINT 'Loading the Bronze Layer...';
		PRINT '==============================================================';
		PRINT 'Loading CRM Tables...';
		PRINT '--------------------------------------------------------------';
		PRINT '>>> Truncating bronze.crm_cust_info table';
		PRINT '--------------------------------------------------------------';

		SET @start_time= GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\Data Analysis\Portfolio Projects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW= 2,
			FIELDTERMINATOR= ',',
			TABLOCK
		);
		SET @end_time= GETDATE();
		PRINT'>>> Load Duration: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT'---------------------';
		PRINT '>>> Truncating bronze.crm_prd_info table';
		PRINT '--------------------------------------------------------------';

		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\Data Analysis\Portfolio Projects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK);
		SET @end_time=GETDATE();
		PRINT'>>> Load Duration: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>> Truncating bronze.crm_sales_details table';
		PRINT '--------------------------------------------------------------';

		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\Data Analysis\Portfolio Projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>>Load Duration: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR)+'seconds';
		PRINT'---------------------';

		PRINT '>>> Truncating bronze.erp_cust_az12';
		PRINT '--------------------------------------------------------------';

		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\Data Analysis\Portfolio Projects\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>>Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'seconds';

		PRINT '>>> Truncating bronze.erp_loc_a101';
		PRINT '--------------------------------------------------------------';

		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\Data Analysis\Portfolio Projects\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>>Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'seconds';

		PRINT '>>> Truncating bronze.erp_px_cat_g1v2';
		PRINT '--------------------------------------------------------------';

		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\Data Analysis\Portfolio Projects\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time=GETDATE();
		SET @batch_end=GETDATE();
		PRINT '>>Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+'seconds';
		PRINT'Load Duration of Whole batch in Bronze Layer: '+CAST(DATEDIFF(SECOND,@batch_start,@batch_end) AS NVARCHAR)+'seconds';
		END TRY

		BEGIN CATCH
			PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER ';
			PRINT 'Error Message' + ERROR_MESSAGE();
			PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		END CATCH
END



