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
		CREATE TABLE silver.cbs_accounts (
			account_id NVARCHAR(5),
			customer_id NVARCHAR(50),
			account_type NVARCHAR(50),
			current_balance NVARCHAR(50),
			credit_limit NVARCHAR(50),
			interest_rate NVARCHAR(50),
			branch_id NVARCHAR(5),
			dwh_create_date DATETIME DEFAULT GETDATE()
		);

		IF OBJECT_ID('silver.cbs_products' , 'U') IS NOT NULL
			DROP TABLE silver.cbs_products;
		CREATE TABLE silver.cbs_products (
			product_id NVARCHAR(5),
			customer_id	NVARCHAR(5),
			product_name NVARCHAR(50),	
			start_date NVARCHAR(50),
			end_date NVARCHAR(50),
			status NVARCHAR(50),
			dwh_create_date DATETIME DEFAULT GETDATE()
		);

		IF OBJECT_ID('silver.cbs_transactions' , 'U') IS NOT NULL
			DROP TABLE silver.cbs_transactions;
		CREATE TABLE silver.cbs_transactions (
			transaction_id NVARCHAR(5),
			customer_id NVARCHAR(5),
			transaction_date NVARCHAR(50),	
			transaction_type NVARCHAR(50),
			amount NVARCHAR(50),
			channel NVARCHAR(50),
			merchant_category NVARCHAR(50),
			dwh_create_date DATETIME DEFAULT GETDATE()
		);

	
		IF OBJECT_ID('silver.crm_customers' , 'U') IS NOT NULL
			DROP TABLE silver.crm_customers;
		CREATE TABLE silver.crm_customers (
			customer_id NVARCHAR(50),
			name NVARCHAR(50),
			gender NVARCHAR(50),
			age INT,
			city NVARCHAR(50),
			annual_income NVARCHAR(50),
			primary_account_type NVARCHAR(50),
			join_date NVARCHAR(50),
			marital_status NVARCHAR(50),
			employment_status NVARCHAR(50),
			dwh_create_date DATETIME DEFAULT GETDATE()
		);

	
		IF OBJECT_ID('silver.erp_branches' , 'U') IS NOT NULL
			DROP TABLE silver.erp_branches;
		CREATE TABLE silver.erp_branches (
			branch_id NVARCHAR(5),
			city NVARCHAR(50),
			num_customers VARCHAR(20),
			manager_name NVARCHAR(50),
			dwh_create_date DATETIME DEFAULT GETDATE()
		);
