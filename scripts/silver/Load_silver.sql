/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY 
        SET @batch_start_time = GETDATE();
        PRINT'====================================================';
        PRINT'Loading Silver Layer';
        PRINT'====================================================';
       
        PRINT'----------------------------------------------------';
        PRINT'Loading CRM Tables';
        PRINT'----------------------------------------------------';

        -- Loading silver.crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.erp_px_cat_g1v2
        PRINT '>> Inserting Date Into: silver.crm_cust_info';

        INSERT INTO silver.crm_cust_info(
               [cst_id]
              ,[cst_key]
              ,[cst_firstname]
              ,[cst_lastname]
              ,[cst_marital_status]
              ,[cst_gndr]
              ,[cst_create_date]
        )

        SELECT [cst_id]
              ,[cst_key]
              ,TRIM(cst_firstname) as cst_firstname -- Remove the spaces
              ,TRIM(cst_lastname) as cst_lastname  -- Remove the spaces

              --Normalizing the marital Stauts to a friendly formate
              ,CASE 
                  WHEN UPPER(TRIM(cst_marital_status)) = 'M' Then 'Married' 
                  WHEN UPPER(TRIM(cst_marital_status)) = 'S' Then 'Single'
                  ELSE 'N/A'
              END as cst_marital_status

              --Normalizing the Gender to a friendly formate
              ,CASE 
                  WHEN UPPER(TRIM(cst_gndr)) = 'M' Then 'Male' 
                  WHEN UPPER(TRIM(cst_gndr)) = 'F' Then 'Female'
                  ELSE 'N/A'
              END as cst_gndr
              ,[cst_create_date]

              -- This Subquery filters all the records where the id is null, and in case the id is repeated, 
              -- it will keep only the latest one by the creation date
          FROM (
                 SELECT *,
                 row_number() over(partition by cst_id order by cst_create_date DESC) as flag
                 from bronze.crm_cust_info
                )t WHERE flag = 1 AND cst_id IS NOT NULL

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
        -- =======================================================================================
        -- =======================================================================================
        -- =======================================================================================
        -- Re-define the table structure in the silver layer 
        /*IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
            DROP TABLE silver.crm_prd_info;
        GO
        CREATE TABLE silver.crm_prd_info (
            prd_id          INT,
            cat_id          NVARCHAR(50),
            prd_key         NVARCHAR(50),
            prd_nm          NVARCHAR(50),
            prd_cost        INT,
            prd_line        NVARCHAR(50),
            prd_start_dt    DATE,
            prd_end_dt      DATE,
            dwh_create_date DATETIME2 DEFAULT GETDATE()
        );*/
        -- Loading silver.crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info
        PRINT '>> Inserting Date Into: silver.crm_prd_info';

        INSERT INTO silver.crm_prd_info(
               [prd_id]
              ,[cat_id]
              ,[prd_key]
              ,[prd_nm]
              ,[prd_cost]
              ,[prd_line]
              ,[prd_start_dt]
              ,[prd_end_dt]
          )
        SELECT
            prd_id,
            --spiliting the prd_key column into two cloumns (1) category id and (2) product key
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id, -- derived column
            SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key, -- derived column

            prd_nm,
            -- replacing all the nulls in the product cose columns with Zero
            ISNULL(prd_cost,0) as prd_cost, 

            -- Normalizing the product line column with a friendly formate 
                CASE UPPER(TRIM(prd_line))
                    WHEN 'M' THEN 'Mountain'
                    WHEN 'R' THEN 'Road'
                    WHEN 's' THEN 'Other Sales'
                    WHEN 'T' THEN 'Touring'
                    ELSE 'n/a' 
                END as prd_line,

            -- Casting the date and removing the time stamp
            CAST(prd_start_dt AS DATE ) as prd_start_dt,

            -- to avoid any gaps and overlapping in dates for each product, I calculated the end_date as "LEAD(start_date) -1"
            CAST(LEAD(prd_start_dt) OVER(PARTITION BY  prd_key ORDER BY prd_start_dt)-1 AS DATE ) as prd_end_dt

        FROM bronze.crm_prd_info;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
         -- =======================================================================================
         -- =======================================================================================
         -- Re-Define the Table Structre as we created new columns
         /*IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
            DROP TABLE silver.crm_sales_details;

        CREATE TABLE silver.crm_sales_details (
            sls_ord_num  NVARCHAR(50),
            sls_prd_key  NVARCHAR(50),
            sls_cust_id  INT,
            sls_order_dt DATE,
            sls_ship_dt  DATE,
            sls_due_dt   DATE,
            sls_sales    INT,
            sls_quantity INT,
            sls_price    INT,
            dwh_create_date DATETIME2 DEFAULT GETDATE()
        );*/
        -- Loading crm_sales_details
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details
        PRINT '>> Inserting Date Into: silver.crm_sales_details';

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

        SELECT  
               [sls_ord_num]
              ,[sls_prd_key]
              ,[sls_cust_id]
              ,
              -- Handling quality issues in order date column 
              CASE 
                  WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                  ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
              END as sls_order_dt

              -- Handling quality issues in ship date column 
              ,  CASE 
                  WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                  ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
              END as sls_ship_dt  

              -- Handling quality issues in due date column 
              ,  CASE 
                  WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                  ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
              END as sls_due_dt  

              -- handling issues in the sales column
              ,CASE 
                   WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * sls_price 
                   THEN sls_quantity * ABS(sls_price)
                   ELSE  sls_sales
               END as sls_sales

              ,[sls_quantity]

              -- Handling the Sales price values and re-calculating it
              ,CASE 
                   WHEN sls_price IS NULL OR sls_price <= 0 
                        THEN sls_sales / NULLIF(sls_quantity, 0)
                   ELSE sls_price
               END as sls_price
     
          FROM [DataWarehouse].[bronze].[crm_sales_details]

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
        -- =======================================================================================
        -- =======================================================================================
        -- =======================================================================================
        PRINT'----------------------------------------------------';
        PRINT'Loading ERP Tables';
        PRINT'----------------------------------------------------';
        -- Loading erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12
        PRINT '>> Inserting Date Into: silver.erp_cust_az12';

        INSERT INTO silver.erp_cust_az12(
            cid,
            bdate,
            gen
        )
        SELECT 
                -- Removing invalied values "NAS" in the cid column  
               CASE
                   WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
                   ELSE cid
               END AS cid

               -- Replacing invalied birthday dates (greater than today) with NULL
              , CASE
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
                END

                -- Normalizing  the gender column to a friendly formate
              ,CASE 
                   WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                   WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                   ELSE 'n/a'
               END as gen

          FROM [DataWarehouse].[bronze].[erp_cust_az12]

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- =======================================================================================
        -- =======================================================================================
        -- =======================================================================================
        -- Loading erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101
        PRINT '>> Inserting Date Into: silver.erp_loc_a101';

        INSERT INTO silver.erp_loc_a101(
            cid,
            cntry
        )

        SELECT 
        REPLACE(cid, '-', '') as cid,
            CASE 
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE cntry
            END as cntry
        FROM bronze.erp_loc_a101
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
        -- =======================================================================================
        -- =======================================================================================
        -- =======================================================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2
        PRINT '>> Inserting Date Into: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2(
            id,
            cat,
            subcat,
            maintenance
        )

        SELECT
            id,
            cat,
            subcat,
            maintenance
        FROM bronze.erp_px_cat_g1v2
        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
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



