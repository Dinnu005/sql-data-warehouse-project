/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;
    DECLARE @crm_start_time DATETIME, @crm_end_time DATETIME;
    DECLARE @erp_start_time DATETIME, @erp_end_time DATETIME;

    -- ==============================================================
    PRINT '================================================';
    PRINT 'Loading Bronze Layer';
    PRINT '================================================';

    SET @batch_start_time = GETDATE();

    -- --------------------------------------------------------------
    PRINT '------------------------------------------------';
    PRINT 'Loading CRM Tables';
    PRINT '------------------------------------------------';
    SET @crm_start_time = GETDATE();

    TRUNCATE TABLE bronze.crm_cust_info;
    BULK INSERT bronze.crm_cust_info
    FROM 'C:\Users\dines\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH(
        FIRSTROW=2,
        FIELDTERMINATOR=',',
        TABLOCK
    );

    TRUNCATE TABLE bronze.crm_prd_info;
    BULK INSERT bronze.crm_prd_info
    FROM 'C:\Users\dines\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH(
        FIRSTROW=2,
        FIELDTERMINATOR=',',
        TABLOCK
    );

    TRUNCATE TABLE bronze.crm_sales_details;
    BULK INSERT bronze.crm_sales_details
    FROM 'C:\Users\dines\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH(
        FIRSTROW=2,
        FIELDTERMINATOR=',',
        TABLOCK
    );

    SET @crm_end_time = GETDATE();
    PRINT 'CRM Load Duration: ' + CAST(DATEDIFF(SECOND, @crm_start_time, @crm_end_time) AS NVARCHAR) + ' seconds';

    -- --------------------------------------------------------------
    PRINT '------------------------------------------------';
    PRINT 'Loading ERP Tables';
    PRINT '------------------------------------------------';
    SET @erp_start_time = GETDATE();

    TRUNCATE TABLE bronze.erp_cust_az12;
    BULK INSERT bronze.erp_cust_az12
    FROM 'C:\Users\dines\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
    WITH(
        FIRSTROW=2,
        FIELDTERMINATOR=',',
        TABLOCK
    );

    TRUNCATE TABLE bronze.erp_loc_a101;
    BULK INSERT bronze.erp_loc_a101
    FROM 'C:\Users\dines\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
    WITH(
        FIRSTROW=2,
        FIELDTERMINATOR=',',
        TABLOCK
    );

    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    BULK INSERT bronze.erp_px_cat_g1v2
    FROM 'C:\Users\dines\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
    WITH(
        FIRSTROW=2,
        FIELDTERMINATOR=',',
        TABLOCK
    );

    SET @erp_end_time = GETDATE();
    PRINT 'ERP Load Duration: ' + CAST(DATEDIFF(SECOND, @erp_start_time, @erp_end_time) AS NVARCHAR) + ' seconds';

    -- ==============================================================
    SET @batch_end_time = GETDATE();
    PRINT '================================================';
    PRINT 'Bronze Layer Load Completed';
    PRINT 'Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
    PRINT '================================================';
END;
