/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
	- Drop the bronze tables if exist and recreate them'
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
	DECLARE @start_time DATETIME , @end_time DATETIME , @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '===========================================================';
		PRINT ' Loading Bronze Layer';
		PRINT '===========================================================';

		PRINT '-----------------------------------------------------------';
		PRINT ' Loading CBS Tables';
		PRINT '-----------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncatating Table: bronze.cbs_accounts';
		TRUNCATE TABLE bronze.cbs_accounts;

		PRINT '>> Inserting Data Into: bronze.cbs_accounts';
		BULK INSERT bronze.cbs_accounts
		FROM 'C:\SQLData\CBS\accounts_cbs.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------- ';


		SET @start_time = GETDATE();
		PRINT '>> Truncatating Table: bronze.cbs_products';
		TRUNCATE TABLE bronze.cbs_products;
	
		PRINT '>> Inserting Data Into: bronze.cbs_products';
		BULK INSERT bronze.cbs_products
		FROM 'C:\SQLData\CBS\products_cbs.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------- ';

		
		SET @start_time = GETDATE();
		PRINT '>> Truncatating Table: bronze.cbs_transactions';
		TRUNCATE TABLE bronze.cbs_transactions;
	
		PRINT '>> Inserting Data Into: bronze.cbs_transactions';
		BULK INSERT bronze.cbs_transactions
		FROM 'C:\SQLData\CBS\transactions_cbs.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------- ';


		PRINT '-----------------------------------------------------------';
		PRINT ' Loading CRM Tables';
		PRINT '-----------------------------------------------------------';
	
		SET @start_time = GETDATE();
		PRINT '>> Truncatating Table: bronze.crm_customers';
		TRUNCATE TABLE bronze.crm_customers;
	
		PRINT '>> Inserting Data Into: bronze.crm_customers';
		BULK INSERT bronze.crm_customers
		FROM 'C:\SQLData\CRM\customers_crm.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------- ';


		PRINT '-----------------------------------------------------------';
		PRINT ' Loading ERP Tables';
		PRINT '-----------------------------------------------------------';
	
		SET @start_time = GETDATE();
		PRINT '>> Truncatating Table: bronze.erp_branches';
		TRUNCATE TABLE bronze.erp_branches;
	
		PRINT '>> Inserting Data Into: bronze.erp_branches';
		BULK INSERT bronze.erp_branches
		FROM 'C:\SQLData\ERP\branches_erp.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------- ';

		SET @batch_end_time = GETDATE();
		PRINT '===========================================================';
		PRINT ' Loading Bronze Layer is Completed';
		PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT '===========================================================';
	END TRY
	BEGIN CATCH
		PRINT '===========================================================';
		PRINT ' ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT ' Error Message' + ERROR_MESSAGE();
		PRINT ' Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT ' Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '===========================================================';
	END CATCH
END
