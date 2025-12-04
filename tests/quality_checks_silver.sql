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


-- ========================================================
-- bronze.cbs_accounts
-- ========================================================

-- Check For Nulls or Duplicates in primary key
-- Expectation: No Result
SELECT account_id, COUNT(*)
FROM bronze.cbs_accounts
GROUP BY account_id
HAVING COUNT(*) > 1 OR account_id IS NULL;

-- Check for unwanted spaces
-- Expectation: No Result
SELECT *
FROM bronze.cbs_accounts;

SELECT customer_id
FROM bronze.cbs_accounts
WHERE customer_id != TRIM(customer_id);
 
SELECT account_type
FROM bronze.cbs_accounts
WHERE account_type != TRIM(account_type);

SELECT current_balance
FROM bronze.cbs_accounts
WHERE current_balance != TRIM(current_balance);

SELECT credit_limit
FROM bronze.cbs_accounts
WHERE credit_limit != TRIM(credit_limit);

SELECT interest_rate
FROM bronze.cbs_accounts
WHERE interest_rate != TRIM(interest_rate);

SELECT branch_id
FROM bronze.cbs_accounts
WHERE branch_id != TRIM(branch_id);


--Data Standardization & Consistency
SELECT DISTINCT account_type
FROM bronze.cbs_accounts;

SELECT *
FROM bronze.cbs_accounts
WHERE TRY_CAST(current_balance AS FLOAT) IS NULL
	  AND current_balance IS NOT NULL;

SELECT *
FROM bronze.cbs_accounts
WHERE TRY_CAST(credit_limit AS FLOAT) IS NULL
	  AND credit_limit IS NOT NULL;

SELECT *
FROM bronze.cbs_accounts
WHERE TRY_CAST(interest_rate AS FLOAT) IS NULL
	  AND interest_rate IS NOT NULL;



-- ========================================================
-- bronze.cbs_products
-- ========================================================

-- Check For Nulls or Duplicates in primary key
-- Expectation: No Result
SELECT product_id, COUNT(*)
FROM bronze.cbs_products
GROUP BY product_id
HAVING COUNT(*) > 1 OR product_id IS NULL;

-- Check for unwanted spaces
-- Expectation: No Result
SELECT *
FROM bronze.cbs_products;


SELECT product_id
FROM bronze.cbs_products
WHERE product_id != TRIM(product_id);

SELECT customer_id
FROM bronze.cbs_products
WHERE customer_id != TRIM(customer_id);

SELECT product_name
FROM bronze.cbs_products
WHERE product_name != TRIM(product_name);


SELECT start_date
FROM bronze.cbs_products
WHERE start_date != TRIM(start_date);


SELECT end_date
FROM bronze.cbs_products
WHERE end_date != TRIM(end_date);

SELECT status
FROM bronze.cbs_products
WHERE status != TRIM(status);

--Data Standardization & Consistency
SELECT DISTINCT product_name
FROM bronze.cbs_products;

SELECT *
FROM bronze.cbs_products
WHERE product_name IS NULL;

SELECT *
FROM bronze.cbs_products;


SELECT *
FROM bronze.cbs_products
WHERE start_date LIKE '%/%'
	  AND start_date IS NOT NULL;

-- Detect all start_date format
-- Result : mm/dd/yyyy, dd-mm-yyyy, 2021/33/10

SELECT start_date
FROM bronze.cbs_products
WHERE start_date IS NOT NULL
GROUP BY start_date
ORDER BY start_date;

-- Detect all end_date format
-- Result : mm/dd/yyyy, 21 'invalid-date'
SELECT end_date
FROM bronze.cbs_products
WHERE end_date IS NOT NULL
GROUP BY end_date
ORDER BY end_date;

SELECT end_date
FROM bronze.cbs_products
WHERE end_date LIKE 'invalid%'

-- Detect all status value
-- Result: 'Closed', 'Active', '', NULL

SELECT DISTINCT status
FROM bronze.cbs_products;


-- ========================================================
-- bronze.cbs_transactions
-- ========================================================



-- Check For Nulls or Duplicates in primary key
-- Expectation: No Result
SELECT transaction_id, COUNT(*)
FROM bronze.cbs_transactions
GROUP BY transaction_id
HAVING COUNT(*)>1 OR transaction_id IS NULL;

-- Check for unwanted spaces
-- Expectation: No Result
-- Result: merchant_category != TRIM(merchant_category)
SELECT *
FROM bronze.cbs_transactions;

SELECT merchant_category
FROM bronze.cbs_transactions
WHERE merchant_category != TRIM(merchant_category);

--Data Standardization & Consistency

-- Detect all transaction_date format
-- Result: dd/mm/yyyy , 113 '2022-25-99'
SELECT transaction_date
FROM bronze.cbs_transactions
WHERE transaction_date IS NOT NULL
GROUP BY transaction_date
ORDER BY transaction_date;

SELECT transaction_date
FROM bronze.cbs_transactions
WHERE transaction_date LIKE '%-%'

-- Detect all transaction_type
-- Result: 'wd' 'REVERSAL' 'FEE' 'CARD_PAYMENT' 'Payment' 'TRANSFER' 'WITHDRAWAL' 'deposit'
SELECT DISTINCT transaction_type
FROM bronze.cbs_transactions


-- Change amount format
SELECT transaction_type,amount
FROM bronze.cbs_transactions
WHERE amount LIKE '-%'

-- Result: error, N/A, -42.5
SELECT amount
FROM bronze.cbs_transactions
WHERE amount IS NOT NULL
GROUP BY amount
ORDER BY amount

-- Detect all Channels
-- Result: ONLINE atm NULL BRANCH MOBILE_APP POS
SELECT DISTINCT channel
FROM bronze.cbs_transactions

-- Detect all merchant_category
-- Result:
SELECT DISTINCT merchant_category
FROM bronze.cbs_transactions

-- ========================================================
-- bronze.crm_customers
-- ========================================================

SELECT *
FROM bronze.crm_customers

-- Check For Nulls or Duplicates in primary key
-- Expectation: No Result
-- There is a lot of duplicates in customer_id but not in the table in general
--So the id is not valid. We need to auto increment a primary key to create a composite key with customer_id

SELECT customer_id , COUNT(*)
FROM bronze.crm_customers
GROUP BY customer_id
HAVING COUNT(*) > 1 OR customer_id IS NULL;


SELECT customer_id, name, COUNT(*)
FROM bronze.crm_customers
GROUP BY customer_id, name
HAVING COUNT(*) > 1 OR name IS NULL;


SELECT *
FROM bronze.crm_customers
WHERE customer_id = 'C79'

SELECT *
FROM bronze.crm_customers
WHERE name = 'Customer_HJKZ'

/*SELECT 
IDENTITY(INT,1,1) AS customer_key,
customer_id
FROM bronze.crm_customers;
*/

-- Check for unwanted spaces
-- Expectation: No Result

SELECT *
FROM bronze.crm_customers

SELECT city, TRIM(city)
FROM bronze.crm_customers
WHERE city != TRIM(city);

SELECT employment_status
FROM bronze.crm_customers
WHERE employment_status != TRIM(employment_status);

--Data Standardization & Consistency
-- Gender Column
-- Result : NULL, F, Male, FEMALE, Other, M
SELECT DISTINCT gender
FROM bronze.crm_customers

-- Age Column
-- Result : NULL, 15-120
SELECT age
FROM bronze.crm_customers
ORDER BY age DESC;

-- City Column
/* Result : NULL, Arvada, Aurora, Boulder, Brighton, castle rock
CastleRock, Centennial, COLORADO SPRINGS, ColoradoSprings, Coloraod, 
Commerce City, CommerceCity, Denver, Englewood, Fort Collins, FortCollins, Greeley
HIGHLANDS RANCH, HighlandsRanch, Lakewood, Littleton, longmont, Loveland
Pueblo, thornton, westminster
*/
SELECT DISTINCT TRIM(city)
FROM bronze.crm_customers

-- Annual income Column
-- Result :not available, NULL, "-- 
SELECT annual_income
FROM bronze.crm_customers
GROUP BY annual_income
ORDER BY annual_income;

-- Primary account type Column
-- Result : Business, Checking, CHK, Premium, SAVINGS, SVG, ---"
SELECT DISTINCT primary_account_type
FROM bronze.crm_customers

SELECT *
FROM bronze.crm_customers
WHERE primary_account_type LIKE '%"'

-- Join date Column
-- Result : 
SELECT DISTINCT join_date
FROM bronze.crm_customers
GROUP BY join_date
ORDER BY join_date

SELECT *
FROM bronze.crm_customers
WHERE join_date IN ('CHECKING', 'CHK','Premium', 'SAVINGS', 'SVG')

-- marital status Column
-- Result : Divorced, Married, Single, Unknown, Widowed, date, NULL
SELECT DISTINCT marital_status
FROM bronze.crm_customers

-- employment status Column
-- Result : NULL




-- ========================================================
-- bronze.erp_branches
-- ========================================================
SELECT *
FROM bronze.erp_branches

-- Check For Nulls or Duplicates in primary key
-- Expectation: No Result
SELECT branch_id, COUNT(*)
FROM bronze.erp_branches
GROUP BY  branch_id
HAVING COUNT(*) > 1 OR  branch_id IS NULL;

-- Check for unwanted spaces
-- Expectation: No Result
SELECT *
FROM bronze.erp_branches;

SELECT city
FROM bronze.erp_branches
WHERE city != TRIM(city);

SELECT manager_name
FROM bronze.erp_branches
WHERE manager_name != TRIM(manager_name);

--Data Standardization & Consistency
SELECT DISTINCT city
FROM bronze.erp_branches

SELECT DISTINCT num_customers
FROM bronze.erp_branches

SELECT *
FROM bronze.erp_branches
WHERE num_customers LIKE '%[^0-9]%'

SELECT DISTINCT manager_name
FROM bronze.erp_branches
