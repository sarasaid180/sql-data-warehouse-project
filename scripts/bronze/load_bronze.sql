

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	BEGIN TRY
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\My Projects\SQL Projects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		With(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)

		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info
		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\My Projects\SQL Projects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		With(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)


		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\My Projects\SQL Projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		With(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

		PRINT '>> Truncating Table: bronze.erp_CUST_AZ12';
		TRUNCATE TABLE bronze.erp_CUST_AZ12
		PRINT '>> Inserting Data Into: bronze.erp_CUST_AZ12';
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'D:\My Projects\SQL Projects\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		With(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)


		PRINT '>> Truncating Table: bronze.erp_LOC_A101'; 
		TRUNCATE TABLE bronze.erp_LOC_A101
		PRINT '>> Inserting Data Into: bronze.erp_LOC_A101';
		BULK INSERT bronze.erp_LOC_A101
		FROM 'D:\My Projects\SQL Projects\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		With(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)


		PRINT '>> Truncating Table: bronze.erp_PX_CAT_G1V2';
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2
		PRINT '>> Inserting Data Into: bronze.erp_PX_CAT_G1V2';
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'D:\My Projects\SQL Projects\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		With(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END





