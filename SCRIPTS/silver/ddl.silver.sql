DECLARE @START_TIME  DATETIME, @END_TIME DATETIME,@START_BATCHTIME DATETIME,@END_BATCHTIME DATETIME;
	print '===================================';
			PRINT 'We are loading the silver_layer';
			print '===================================';

			print '===================================';
			PRINT 'loading CRM Tables';
			print '===================================';
	         SET @START_BATCHTIME=GETDATE();
	PRINT '>>TRUNCATING table:silver.crm_cust_info';
	         SET @START_TIME=GETDATE()
			 TRUNCATE TABLE silver.crm_cust_info;
INSERT INTO silver.crm_cust_info (cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gender,
	cst_create_date)
	SELECT 
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE
		WHEN UPPER(TRIM(cst_marital_status))='S' then 'SINGLE'
		WHEN UPPER(TRIM(cst_marital_status))='M' then 'MARRIED'
		ELSE 'n/a'
	END cst_marital_status,
	CASE
		WHEN UPPER(TRIM(cst_gender))='F' then 'FEMALE'
		WHEN UPPER(TRIM(cst_gender))='M' then 'MALE'
		ELSE 'n/a'
	END cst_gender,
	cst_create_date
	FROM (SELECT *, ROW_NUMBER() over (PARTITION BY cst_id order by cst_create_date desc) as flag_last
	from bronze.crm_cust_info
	where cst_id is not null
	) t
	where flag_last=1
	SET @END_TIME=GETDATE()
			PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME)AS NVARCHAR)+'SECONDS';
			PRINT '>>>.........<<<'

	IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
	   DROP TABLE silver.crm_prd_info;
	CREATE TABLE silver.crm_prd_info(
	prd_id int,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
	);
	SET @START_TIME=GETDATE()
	PRINT '>>TRUNCATING table:silver.crm_prd_info';
				TRUNCATE TABLE silver.crm_prd_info;
	insert into silver.crm_prd_info(prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt)
	SELECT  prd_id,
		  REPLACE(substring(prd_key,1,5),'-','_') as cat_id,
		  SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
		  prd_nm,
		  isnull(prd_cost,0) as prd_cost,
		  CASE WHEN UPPER(TRIM(prd_line))='M' THEN 'MOUNTAIN'
			   WHEN UPPER(TRIM(prd_line))='R' THEN 'ROAD'
			   WHEN UPPER(TRIM(prd_line))='S' THEN 'OTHER SALES'
			   WHEN UPPER(TRIM(prd_line))='T' THEN 'TOURING'
			   ELSE 'N/A'
		  END as prd_line,
		  CAST(prd_start_dt AS DATE) AS prd_start_dt,
		  LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as prd_end_dt
	  FROM [DataWarehouse].[bronze].[crm_prd_info]
	  SET @END_TIME=GETDATE()
			PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME)AS NVARCHAR)+'SECONDS';
			PRINT '>>>.........<<<'

	  IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
	   DROP TABLE silver.crm_sales_details;
	CREATE TABLE silver.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
	);
	SET @START_TIME=GETDATE()
	PRINT '>>TRUNCATING table: silver.crm_sales_details';
				TRUNCATE TABLE  silver.crm_sales_details;
	INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)
	  select 
	  sls_ord_num,
	  sls_prd_key,
	  sls_cust_id,
	  CASE when sls_order_dt=0 or LEN(sls_order_dt) != 8 THEN NULL
		   ELSE CAST (CAST(sls_order_dt AS varchar) AS DATE)
	  END sls_order_dt,
	  CASE when sls_ship_dt =0 or LEN(sls_ship_dt) != 8 THEN NULL
		   ELSE CAST (CAST(sls_ship_dt AS varchar) AS DATE)
	  END sls_ship_dt,
	   CASE when sls_due_dt=0 or LEN(sls_due_dt) != 8 THEN NULL
		   ELSE CAST (CAST(sls_due_dt AS varchar) AS DATE)
	  END sls_due_dt,
	  CASE WHEN sls_sales IS NULL or sls_sales<=0 OR sls_sales != sls_quantity*ABS(sls_price)
			  THEN sls_quantity*ABS(sls_price)
			  ELSE sls_sales
	  END AS sls_sales,
	  sls_quantity,
	  CASE WHEN sls_price IS NULL OR sls_price<=0
		   THEN sls_sales/ NULLIF(sls_quantity,0)
	  ELSE sls_price
	  END sls_price
	  from bronze.crm_sales_details
	   SET @END_TIME=GETDATE()
			PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME)AS NVARCHAR)+'SECONDS';
			PRINT '>>>.........<<<'

       SET @START_TIME=GETDATE()
	   print '===================================';
			PRINT 'loading ERP Tables';
			print '===================================';
	  PRINT '>>TRUNCATING table:silver.erp_cust_az12';
				TRUNCATE TABLE silver.erp_cust_az12 ;
	INSERT INTO silver.erp_cust_az12(
	cid,
	bdate,
	gen)
	  select CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
				  else cid
			 end  as cid,
	  CASE WHEN bdate>GETDATE() THEN NULL
			 ELSE bdate
		 end as bdate,
	  CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
		   WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
		   ELSE 'N/A'
	  END as gen
	  from bronze.erp_cust_az12
	    SET @END_TIME=GETDATE()
			PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME)AS NVARCHAR)+'SECONDS';
			PRINT '>>>.........<<<'

			SET @START_TIME=GETDATE()
	  PRINT '>>TRUNCATING table:silver.erp_loc_a101';
				TRUNCATE TABLE silver.erp_loc_a101;
	INSERT INTO silver.erp_loc_a101(
	cid,cntry)
	  select REPLACE (cid,'-','') as cid,
	  CASE WHEN TRIM(cntry) ='DE' THEN 'GERMANY'
		   WHEN TRIM(cntry) IN ('US','USA') THEN 'UNITED STATES'
		   WHEN TRIM(cntry) ='' or cntry IS NULL THEN 'N/A'
		   ELSE cntry
	 END AS cntry
	  from bronze.erp_loc_a101
	  SET @END_TIME=GETDATE()
			PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME)AS NVARCHAR)+'SECONDS';
			PRINT '>>>.........<<<'

			SET @START_TIME=GETDATE()
	  PRINT '>>TRUNCATING table:silver.erp_px_cat_g1v2';
				TRUNCATE TABLE silver.erp_px_cat_g1v2;
	INSERT INTO silver.erp_px_cat_g1v2(
	id,cat,subcat,maintenance)
	  SELECT id,cat,subcat,maintenance FROM bronze.erp_px_cat_g1v2
	   SET @END_TIME=GETDATE()
			PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME)AS NVARCHAR)+'SECONDS';
			PRINT '>>>.........<<<'
		SET @END_BATCHTIME=GETDATE()
			PRINT '>> BATCH LOAD DURATION:' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME)AS NVARCHAR)+'SECONDS';
			PRINT '>>>.........<<<'
