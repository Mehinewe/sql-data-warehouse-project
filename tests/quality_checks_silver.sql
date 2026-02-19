/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/
-- Check for nulls or Duplicates in Primary Key
-- Expectation: No Result
SELECT 
cst_id,
COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for unwanted spaces for string values
-- Expectation: No Result
SELECT cst_firstname 
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname 
FROM silver.crm_cust_info
WHERE cst_lastname  != TRIM(cst_lastname )

SELECT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr  != TRIM(cst_gndr )


-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info

SELECT *
FROM silver.crm_cust_info


-------------------------------------------------------------
-- silver.crm_prd_info
-------------------------------------------------------------
-- Check for nulls or Duplicates in Primary Key
-- Expectation: No Result
SELECT 
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for unwanted spaces for string values
-- Expectation: No Result
SELECT prd_nm
FROM silver.crm_prd_info
WHERE TRIM(prd_nm) != prd_nm;

-- Check for NULLS or Negatibe Numbers
-- Expectation: No Result
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Check for Invalide Orders Date
SELECT * 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

-------------------------------------------------------------
-- silver.crm_sales_details
-------------------------------------------------------------
-- Check for invalid dates
SELECT 
ISNULL(sls_order_dt, 0) 
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20260218
OR sls_order_dt < 19000101;

SELECT 
ISNULL(sls_ship_dt, 0)
FROM silver.crm_sales_details
WHERE sls_ship_dt <= 0 
OR LEN(sls_ship_dt) != 8
OR sls_ship_dt > 20260218
OR sls_ship_dt < 19000101;

SELECT 
ISNULL(sls_due_dt, 0)
FROM silver.crm_sales_details
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20260218
OR sls_due_dt < 19000101;

-- Check date chronologie
SELECT * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
OR sls_order_dt > sls_due_dt;

-- Check date concistency : Between Sales , Quantity and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero or negative.
SELECT DISTINCT
sls_sales,
sls_quantity,		
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
 OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY
sls_sales,
sls_quantity,		
sls_price


-------------------------------------------------------------
-- silver.erp_cust_az12
-------------------------------------------------------------
-- Identify Out-Of-Range Dates
SELECT DISTINCT 
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1926-01-01' OR bdate > GETDATE()

-- Data Standardization & Consistency
SELECT DISTINCT
gen
FROM silver.erp_cust_az12

SELECT *
FROM silver.erp_cust_az12


-------------------------------------------------------------
-- silver.erp_loc_a101
-------------------------------------------------------------
-- Data Standardization & Consistency
SELECT DISTINCT 
cntry
FROM silver.erp_loc_a101
ORDER BY cntry

SELECT * FROM silver.erp_loc_a101

-------------------------------------------------------------
-- silver.erp_px_cat_g1v2
-------------------------------------------------------------

-- Check for unwanted spaces
SELECT * FROM silver.erp_px_cat_g1v2 
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- Data Standardization & Consistency
SELECT DISTINCT cat FROM silver.erp_px_cat_g1v2

SELECT DISTINCT 
subcat 
FROM silver.erp_px_cat_g1v2
ORDER BY subcat 

SELECT DISTINCT maintenance FROM silver.erp_px_cat_g1v2

SELECT DISTINCT * FROM silver.erp_px_cat_g1v2






