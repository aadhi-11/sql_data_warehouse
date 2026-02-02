/*
===============================================================================
Stored Procedure: silver.load_silver
===============================================================================
Purpose:
    Load data from Bronze layer into Silver layer by applying
    cleansing, normalization, deduplication, and business rules.

Actions Performed:
    - Truncates Silver tables
    - Transforms and loads data from Bronze → Silver
    - Tracks load duration per table and overall batch
    - Handles errors using TRY / CATCH

Parameters:
    None

Usage:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    DECLARE 
        @batch_start_time DATETIME,
        @batch_end_time   DATETIME,
        @start_time       DATETIME,
        @end_time         DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '================================================';
        PRINT 'Starting Silver Layer Load';
        PRINT '================================================';

        /* ===============================================================
           CRM CUSTOMER INFO
           - Remove duplicate customers using ROW_NUMBER
           - Trim string columns
           - Expand marital status & gender codes
           - Remove NULL cst_id early for data integrity & performance
           =============================================================== */

        SET @start_time = GETDATE();
        PRINT '>> Loading: silver.crm_cust_info';

        TRUNCATE TABLE silver.crm_cust_info;

        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_first_name,
            cst_last_name,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_first_name),
            TRIM(cst_last_name),
            CASE 
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                ELSE 'N/A'
            END,
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'N/A'
            END,
            cst_create_date
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY cst_id 
                       ORDER BY cst_create_date DESC
                   ) AS rn
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE rn = 1;

        SET @end_time = GETDATE();
        PRINT '>> Completed in ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';


        /* ===============================================================
           CRM PRODUCT INFO
           - Derive category id & product key
           - Normalize product line codes
           - Handle NULL cost
           - Fix overlapping date ranges using LEAD
           =============================================================== */

        SET @start_time = GETDATE();
        PRINT '>> Loading: silver.crm_prd_info';

        TRUNCATE TABLE silver.crm_prd_info;

        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key,1,5),'-','_'),
            SUBSTRING(prd_key,7,LEN(prd_key)),
            prd_nm,
            ISNULL(prd_cost,0),
            CASE 
                WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
                WHEN UPPER(TRIM(prd_line))='S' THEN 'Other Sale'
                ELSE 'N/A'
            END,
            CAST(prd_start_dt AS DATE),
            CAST(
                LEAD(prd_start_dt) 
                OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1
                AS DATE
            )
        FROM bronze.crm_prd_info;

        SET @end_time = GETDATE();
        PRINT '>> Completed in ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';


        /* ===============================================================
           CRM SALES DETAILS
           - Clean invalid dates
           - Recalculate incorrect sales
           - Derive missing price values
           =============================================================== */

        SET @start_time = GETDATE();
        PRINT '>> Loading: silver.crm_sales_details';

        TRUNCATE TABLE silver.crm_sales_details;

        INSERT INTO silver.crm_sales_details (
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
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) <> 8 THEN NULL
                 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END,
            CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) <> 8 THEN NULL
                 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END,
            CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) <> 8 THEN NULL
                 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END,
            CASE 
                WHEN sls_sales IS NULL 
                  OR sls_sales <= 0 
                  OR sls_sales <> sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END,
            sls_quantity,
            CASE 
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity,0)
                ELSE sls_price
            END
        FROM bronze.crm_sales_details;

        SET @end_time = GETDATE();
        PRINT '>> Completed in ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';


        /* ===============================================================
           ERP CUSTOMER
           - Remove NAS prefix
           - Handle future birthdates
           - Normalize gender values
           =============================================================== */

        SET @start_time = GETDATE();
        PRINT '>> Loading: silver.erp_cust_az12';

        TRUNCATE TABLE silver.erp_cust_az12;

        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) ELSE cid END,
            CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END,
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
                WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
                ELSE 'N/A'
            END
        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT '>> Completed in ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';


        /* ===============================================================
           ERP LOCATION
           - Remove hyphen from CID
           - Normalize country values
           =============================================================== */

        SET @start_time = GETDATE();
        PRINT '>> Loading: silver.erp_loc_a101';

        TRUNCATE TABLE silver.erp_loc_a101;

        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT
            REPLACE(cid,'-',''),
            CASE 
                WHEN UPPER(TRIM(cntry))='DE' THEN 'Germany'
                WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
                WHEN cntry IS NULL OR TRIM(cntry)='' THEN 'N/A'
                ELSE TRIM(cntry)
            END
        FROM bronze.erp_loc_a101;

        SET @end_time = GETDATE();
        PRINT '>> Completed in ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';


        /* ===============================================================
           ERP PRODUCT CATEGORY (High Quality – Direct Load)
           =============================================================== */

        SET @start_time = GETDATE();
        PRINT '>> Loading: silver.erp_px_cat_g1v2';

        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT id, cat, subcat, maintenance
        FROM bronze.erp_px_cat_g1v2;

        SET @end_time = GETDATE();
        PRINT '>> Completed in ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds';

        SET @batch_end_time = GETDATE();
        PRINT '================================================';
        PRINT 'Silver Layer Load Completed Successfully';
        PRINT 'Total Duration: ' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        PRINT '================================================';
        PRINT 'ERROR DURING SILVER LOAD';
        PRINT ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================================';
    END CATCH
END;
