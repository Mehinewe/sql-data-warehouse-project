/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

		IF OBJECT_ID('silver.cbs_accounts' , 'U') IS NOT NULL
			DROP TABLE silver.cbs_accounts;
		GO

		CREATE TABLE silver.cbs_accounts (
			account_id NVARCHAR(5),
			customer_id NVARCHAR(50),
			account_type NVARCHAR(50),
			current_balance DECIMAL(18,2),
			credit_limit DECIMAL(18,2),
			interest_rate DECIMAL(18,2),
			branch_id NVARCHAR(5),
			dwh_create_date DATETIME DEFAULT GETDATE()
		);

		GO

		IF OBJECT_ID('silver.cbs_products' , 'U') IS NOT NULL
			DROP TABLE silver.cbs_products;
		GO

		CREATE TABLE silver.cbs_products (
			product_id NVARCHAR(5),
			customer_id	NVARCHAR(5),
			product_name NVARCHAR(50),	
			start_date DATE,
			end_date DATE,
			status NVARCHAR(50),
			dwh_create_date DATETIME DEFAULT GETDATE()
		);
		GO

		IF OBJECT_ID('silver.cbs_transactions' , 'U') IS NOT NULL
			DROP TABLE silver.cbs_transactions;
		GO

		CREATE TABLE silver.cbs_transactions (
			transaction_id NVARCHAR(5),
			customer_id NVARCHAR(5),
			transaction_date DATE,	
			transaction_type NVARCHAR(50),
			amount DECIMAL(18,2),
			channel NVARCHAR(50),
			merchant_category NVARCHAR(50),
			dwh_create_date DATETIME DEFAULT GETDATE()
		);
		GO

	
		IF OBJECT_ID('silver.crm_customers' , 'U') IS NOT NULL
			DROP TABLE silver.crm_customers;
		GO

		CREATE TABLE silver.crm_customers (
			customer_id NVARCHAR(50),
			name NVARCHAR(50),
			gender NVARCHAR(50),
			age INT,
			city NVARCHAR(50),
			annual_income DECIMAL(18,2),
			primary_account_type NVARCHAR(50),
			join_date DATE,
			marital_status NVARCHAR(50),
			employment_status NVARCHAR(50),
			dwh_create_date DATETIME DEFAULT GETDATE()
		);
		GO

	
		IF OBJECT_ID('silver.erp_branches' , 'U') IS NOT NULL
			DROP TABLE silver.erp_branches;
		GO

		CREATE TABLE silver.erp_branches (
			branch_id NVARCHAR(5),
			city NVARCHAR(50),
			num_customers VARCHAR(20),
			manager_name NVARCHAR(50),
			dwh_create_date DATETIME DEFAULT GETDATE()
		);
		GO
