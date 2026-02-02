/*
=====================================
stored procedure: Load bronze layer(source>bronze)
script purpose:this loads data into bronze schema from external .csv files,
it truncates the table before loading uses BULK INSERT COMMAND TO LOAD CSV FILES TO BRONZE TABLES.
PARAMETERS : NONE IT DOES NOT ACCEPT ANY PARAMETER OR RETURN ANY VALUE

USAGE : EXEC bronze.load_bronze;

=====================================
*/
create or alter PROCEDURE bronze.load_bronze as
BEGIN
     DECLARE @START_TIME  DATETIME, @END_TIME DATETIME,@START_BATCHTIME DATETIME,@END_BATCHTIME DATETIME;
    BEGIN TRY
	        SET @START_BATCHTIME=GETDATE();
			print '===================================';
			PRINT 'We are loading the bronze_layer';
			print '===================================';

			print '===================================';
			PRINT 'loading CRM Tables';
			print '===================================';
			SET @START_TIME=GETDATE()
			PRINT '>>TRUNCATING table:bronze.crm_cust_info';
			TRUNCATE TABLE bronze.crm_cust_info;
			PRINT '>>INSERTING data Into bronze.crm_cust_info';
			BULK INSERT bronze.crm_cust_info
			FROM "C:\Users\HP\OneDrive\Desktop\SQL DWH\sql-data-warehouse-project\datasets\source_crm\cust_info.csv"
			WITH 
			(FIRSTROW=2,
			FIELDTERMINATOR =',',
			TABLOCK
			);
			SET @END_TIME=GETDATE()
			PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME)AS NVARCHAR)+'SECONDS';
			PRINT '>>>.........<<<'
			SET @START_TIME=GETDATE()
			PRINT '>>TRUNCATING table:bronze.crm_prd_info';
			TRUNCATE TABLE bronze.crm_prd_info;
			PRINT '>>INSERTING data Into bronze.crm_prd_info';
			BULK INSERT bronze.crm_prd_info
			FROM "C:\Users\HP\OneDrive\Desktop\SQL DWH\sql-data-warehouse-project\datasets\source_crm\prd_info.csv"
			WITH 
			(FIRSTROW=2,
			FIELDTERMINATOR =',',
			TABLOCK
			);
			SET @END_TIME=GETDATE()
			PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME)AS NVARCHAR)+'SECONDS';
			PRINT '>>>.........<<<'
			SET @START_TIME=GETDATE();
			PRINT '>>TRUNCATING table:bronze.crm_sales_details';
			TRUNCATE TABLE bronze.crm_sales_details;
			PRINT '>>INSERTING data Into bronze.crm_sales_details';
			BULK INSERT bronze.crm_sales_details
			FROM "C:\Users\HP\OneDrive\Desktop\SQL DWH\sql-data-warehouse-project\datasets\source_crm\sales_details.csv"
			WITH 
			(FIRSTROW=2,
			FIELDTERMINATOR =',',
			TABLOCK
			);
			SET @END_TIME=GETDATE()
			PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME)AS NVARCHAR)+'SECONDS';
			PRINT '>>>.........<<<'
			print '===================================';
			PRINT 'loading ERP Tables';
			print '===================================';
			SET @START_TIME=GETDATE();
			PRINT '>>TRUNCATING table:bronze.erp_cust_az12';
			TRUNCATE TABLE bronze.erp_cust_az12;
			PRINT '>>INSERTING data Into bronze.erp_cust_az12';
			BULK INSERT bronze.erp_cust_az12
			FROM "C:\Users\HP\OneDrive\Desktop\SQL DWH\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv"
			WITH 
			(FIRSTROW=2,
			FIELDTERMINATOR =',',
			TABLOCK
			);
			SET @END_TIME=GETDATE()
			PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME)AS NVARCHAR)+'SECONDS';
			PRINT '>>>.........<<<'
			SET @START_TIME=GETDATE();
			PRINT '>>TRUNCATING table:bronze.erp_loc_a101';
			TRUNCATE TABLE bronze.erp_loc_a101
			PRINT '>>INSERTING data Into bronze.erp_loc_a101';
			BULK INSERT bronze.erp_loc_a101
			FROM "C:\Users\HP\OneDrive\Desktop\SQL DWH\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv"
			WITH 
			(FIRSTROW=2,
			FIELDTERMINATOR =',',
			TABLOCK
			);
			SET @END_TIME=GETDATE()
			PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME)AS NVARCHAR)+'SECONDS';
			PRINT '>>>.........<<<'
			SET @START_TIME=GETDATE();
			PRINT '>>TRUNCATING table:bronze.erp_px_cat_g1v2'
			TRUNCATE TABLE bronze.erp_px_cat_g1v2
			PRINT '>>INSERTING data into bronze.erp_px_cat_g1v2';
			BULK INSERT bronze.erp_px_cat_g1v2
			FROM "C:\Users\HP\OneDrive\Desktop\SQL DWH\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv"
			WITH 
			(FIRSTROW=2,
			FIELDTERMINATOR =',',
			TABLOCK
			);
			SET @END_TIME=GETDATE()
			PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME)AS NVARCHAR)+'SECONDS';
			PRINT '>>>.........<<<'
			SET @END_BATCHTIME=GETDATE()
			PRINT '>> BATCH LOAD DURATION:' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME)AS NVARCHAR)+'SECONDS';
			PRINT '>>>.........<<<'
	END TRY
	BEGIN CATCH
	    PRINT '======================================='
		PRINT 'ERROR OCCURED WHILE LOADING BRONZE LAYER'
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT '======================================='
	END CATCH
END
