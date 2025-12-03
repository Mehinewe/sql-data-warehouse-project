INSERT INTO silver.cbs_accounts (
	account_id,
	customer_id,
	account_type,
	current_balance,
	credit_limit ,
	interest_rate ,
	branch_id 
)

SELECT
account_id ,
customer_id ,
CASE WHEN UPPER(TRIM(account_type)) = 'CHK' THEN 'Checking'
	 WHEN UPPER(TRIM(account_type)) = 'SVG' THEN 'Savings'
	 WHEN UPPER(TRIM(account_type)) = 'SAVINGS' THEN 'Savings'
	 WHEN UPPER(TRIM(account_type)) IS NULL THEN 'n/a'
	 ELSE account_type
END AS account_type,
CASE WHEN UPPER(TRIM(current_balance)) IN ('', 'NaN', 'missing') THEN '0'
	  WHEN UPPER(TRIM(current_balance)) IS NULL THEN '0'
	 ELSE current_balance
END AS current_balance,
CASE WHEN UPPER(TRIM(credit_limit)) IN ('', 'N/A') THEN '0'
	  WHEN UPPER(TRIM(credit_limit)) IS NULL THEN '0'
	 ELSE credit_limit
END AS credit_limit,
ISNULL(interest_rate, 0) AS  interest_rate,
branch_id
FROM bronze.cbs_accounts
WHERE account_id IS NOT NULL;


INSERT INTO silver.cbs_products (
	product_id,
	customer_id,
	product_name,	
	start_date,
	end_date,
	status
)

SELECT
product_id,
customer_id,
CASE WHEN product_name IS NULL THEN 'n/a'
	 ELSE product_name
END AS product_name,	
CASE WHEN start_date LIKE '%/%' THEN TRY_CONVERT (DATE, start_date, 101)
	 WHEN start_date LIKE '%-%' THEN TRY_CONVERT (DATE, start_date, 105)
	 ELSE NULL
END AS start_date,
CASE WHEN end_date LIKE '%/%' THEN TRY_CONVERT (DATE, end_date, 101)
	 ELSE NULL
END AS end_date,
CASE WHEN UPPER(TRIM(status))= 'Active' THEN 'Active'
	 WHEN UPPER(TRIM(status))= 'Closed' THEN 'Closed'
	 WHEN status = '' THEN NULL
	 ELSE status
END AS status
FROM bronze.cbs_products;


INSERT INTO silver.cbs_transactions (
	transaction_id,
	customer_id,
	transaction_date,	
	transaction_type,
	amount,
	channel,
	merchant_category 
)


SELECT
transaction_id,
customer_id,
CASE WHEN transaction_date LIKE '%/%' THEN TRY_CONVERT (DATE, transaction_date, 105)
	 ELSE NULL 
END AS transaction_date,	
CASE WHEN UPPER(TRIM(transaction_type)) = 'wd' THEN 'Withdrawal'
	 WHEN UPPER(TRIM(transaction_type)) = 'REVERSAL' THEN 'Reversal'
	 WHEN UPPER(TRIM(transaction_type)) = 'FEE' THEN 'Fee'
	 WHEN UPPER(TRIM(transaction_type)) = 'CARD_PAYMENT' THEN 'Card Payment'
	 WHEN UPPER(TRIM(transaction_type)) = 'Payment' THEN 'Payment'
	 WHEN UPPER(TRIM(transaction_type)) = 'TRANSFER' THEN 'Transfer'
	 WHEN UPPER(TRIM(transaction_type)) = 'WITHDRAWAL' THEN 'Withdrawal'
	 WHEN UPPER(TRIM(transaction_type)) = 'deposit' THEN 'Deposit'
	 ELSE 'n/a'
END AS transaction_type,
CASE WHEN amount = '-42.5' THEN '42.5'
	 WHEN amount IN ('error', 'N/A','') THEN '0'
	 WHEN amount IS NULL THEN '0'
	 ELSE amount
END AS amount,
CASE WHEN UPPER(TRIM(channel))= 'ONLINE' THEN 'Online'
	 WHEN UPPER(TRIM(channel))= 'atm' THEN 'ATM'
	 WHEN UPPER(TRIM(channel))= 'BRANCH' THEN 'Branch'
	 WHEN UPPER(TRIM(channel))= 'MOBILE_APP' THEN 'Mobile App'
	 WHEN UPPER(TRIM(channel))= 'POS' THEN 'POS' 
	 ELSE 'n/a'
	END AS Channel,
CASE WHEN merchant_category IS NULL OR merchant_category = '' THEN 'n/a' 
ELSE (
	SELECT STRING_AGG( UPPER(LEFT(value,1)) + LOWER(SUBSTRING(value,2, LEN(value))),
	' '
	)
	FROM STRING_SPLIT(TRIM(merchant_category), ' ')
	)
	END AS merchant_category
FROM bronze.cbs_transactions;



INSERT INTO silver.crm_customers (
customer_id,
name ,
gender ,
age ,
city ,
annual_income ,
primary_account_type ,
join_date ,
marital_status ,
employment_status
)



SELECT
customer_id,
name ,
CASE WHEN UPPER(TRIM(gender)) = 'F' THEN 'Female'
	 WHEN UPPER(TRIM(gender)) = 'FEMALE' THEN 'Female'
	 WHEN UPPER(TRIM(gender)) = 'M' THEN 'Male'
	 WHEN UPPER(TRIM(gender)) = 'Male' THEN 'Male'
	 WHEN UPPER(TRIM(gender)) = 'Other' THEN 'Other'
	 ELSE 'n/a'
END AS gender,
CASE WHEN age IS NULL THEN '0'
	 ELSE age
END AS age,
CASE WHEN UPPER(TRIM(city)) = 'Arvada' THEN 'Arvada'
	 WHEN UPPER(TRIM(city)) = 'Aurora' THEN 'Aurora'
	 WHEN UPPER(TRIM(city)) = 'Boulder' THEN 'Boulder'
	 WHEN UPPER(TRIM(city)) = 'Brighton' THEN 'Brighton'
	 WHEN UPPER(TRIM(city)) IN ( 'castle rock', 'CastleRock') THEN 'Castle Rock'
	 WHEN UPPER(TRIM(city)) = 'Centennial' THEN 'Centennial'
	 WHEN UPPER(TRIM(city)) IN ( 'COLORADO SPRINGS', 'ColoradoSprings', 'Coloraod') THEN 'Colorado Springs'
	 WHEN UPPER(TRIM(city)) IN ( 'Commerce City', 'CommerceCity') THEN 'Commerce City'
	 WHEN UPPER(TRIM(city)) = 'Denver' THEN 'Denver'
	 WHEN UPPER(TRIM(city)) = 'Englewood' THEN 'Englewood'
	 WHEN UPPER(TRIM(city)) IN ( 'Fort Collins', 'FortCollins') THEN 'Fort Collins'
	 WHEN UPPER(TRIM(city)) = 'Greeley' THEN 'Greeley'
	 WHEN UPPER(TRIM(city)) IN ( 'HIGHLANDS RANCH', 'HighlandsRanch') THEN 'Highlands Ranch'
	 WHEN UPPER(TRIM(city)) = 'Lakewood' THEN 'Lakewood'
	 WHEN UPPER(TRIM(city)) = 'Littleton' THEN 'Littleton'
	 WHEN UPPER(TRIM(city)) = 'longmont' THEN 'Longmont'
	 WHEN UPPER(TRIM(city)) = 'Loveland' THEN 'Loveland'
	 WHEN UPPER(TRIM(city)) = 'Pueblo' THEN 'Pueblo'
	 WHEN UPPER(TRIM(city)) = 'thornton' THEN 'Thornton'
	 WHEN UPPER(TRIM(city)) = 'westminster' THEN 'Westminster'
	 WHEN city IS NULL THEN 'n/a'
	 ELSE TRIM(city)
END AS city,
CASE WHEN annual_income = 'not available'  OR annual_income IS NULL THEN '0'
	 WHEN annual_income LIKE '"%' AND primary_account_type LIKE '%"' THEN 
																	CONCAT(
																	REPLACE(annual_income, '"', '') 
																	, REPLACE(primary_account_type, '"', '')
																	 )
	 ELSE annual_income
END AS annual_income,
CASE WHEN UPPER(TRIM(primary_account_type)) = 'Business' THEN 'Business'
	 WHEN UPPER(TRIM(primary_account_type)) IN ('Checking', 'CHK')  THEN 'Checking'
	 WHEN UPPER(TRIM(primary_account_type)) = 'Premium' THEN 'Premium'
	 WHEN UPPER(TRIM(primary_account_type)) IN ('SAVINGS', 'SVG')  THEN 'Savings'
	 WHEN primary_account_type IS NULL THEN 'n/a'
	 ELSE -- Some join_date has the account type data
		CASE WHEN UPPER(TRIM(join_date)) = 'Business' THEN 'Business'
			 WHEN UPPER(TRIM(join_date)) IN ('Checking', 'CHK')  THEN 'Checking'
			 WHEN UPPER(TRIM(join_date)) = 'Premium' THEN 'Premium'
			 WHEN UPPER(TRIM(join_date)) IN ('SAVINGS', 'SVG')  THEN 'Savings'
			 ELSE primary_account_type
		END
END AS primary_account_type,
CASE WHEN join_date LIKE '%/%' THEN TRY_CONVERT (DATE, join_date, 101)
	 WHEN join_date IN ('CHECKING', 'CHK','Premium', 'SAVINGS', 'SVG') THEN TRY_CONVERT (DATE, marital_status, 101)
	 ELSE NULL 
END AS join_date ,
CASE WHEN UPPER(TRIM(employment_status)) LIKE 'Divorced%' THEN 'Divorced'
	 WHEN UPPER(TRIM(employment_status)) LIKE 'Married%' THEN 'Married'
	 WHEN UPPER(TRIM(employment_status)) LIKE 'Single%' THEN 'Single'
	 WHEN UPPER(TRIM(employment_status)) LIKE 'Widow%' THEN 'Widowed'
	 WHEN UPPER(TRIM(employment_status)) LIKE 'Unknown%' THEN 'n/a'
	 WHEN marital_status IS NULL THEN 'n/a'
	 ELSE marital_status
END AS marital_status ,
CASE WHEN UPPER(TRIM(employment_status)) LIKE '%EMPLOYED' THEN 'Employed'
	 WHEN UPPER(TRIM(employment_status)) LIKE '%Retired' THEN 'Retired'
	 WHEN UPPER(TRIM(employment_status)) LIKE '%Student' THEN 'Student'
	 WHEN UPPER(TRIM(employment_status)) LIKE '%Self-employed' THEN 'Self-employed'
	 WHEN UPPER(TRIM(employment_status)) LIKE '%Unemployed' THEN 'Unemployed'
	 WHEN UPPER(TRIM(employment_status)) LIKE '%Part-time' THEN 'Employed'
	 WHEN UPPER(TRIM(employment_status)) LIKE '%,Retired' THEN ',Retired'
	 ELSE 'n/a'
END AS employment_status
FROM bronze.crm_customers;





INSERT INTO silver.erp_branches (
branch_id ,
city ,
num_customers ,
manager_name 
)



SELECT
branch_id ,
CASE WHEN UPPER(TRIM(city)) = 'Arvada' THEN 'Arvada'
	 WHEN UPPER(TRIM(city)) = 'Aurora' THEN 'Aurora'
	 WHEN UPPER(TRIM(city)) = 'Boulder' THEN 'Boulder'
	 WHEN UPPER(TRIM(city)) = 'Brighton' THEN 'Brighton'
	 WHEN UPPER(TRIM(city)) IN ( 'castle rock', 'CastleRock') THEN 'Castle Rock'
	 WHEN UPPER(TRIM(city)) = 'Centennial' THEN 'Centennial'
	 WHEN UPPER(TRIM(city)) IN ( 'COLORADO SPRINGS', 'ColoradoSprings', 'Coloraod') THEN 'Colorado Springs'
	 WHEN UPPER(TRIM(city)) IN ( 'Commerce City', 'CommerceCity') THEN 'Commerce City'
	 WHEN UPPER(TRIM(city)) = 'Denver' THEN 'Denver'
	 WHEN UPPER(TRIM(city)) = 'Englewood' THEN 'Englewood'
	 WHEN UPPER(TRIM(city)) IN ( 'Fort Collins', 'FortCollins') THEN 'Fort Collins'
	 WHEN UPPER(TRIM(city)) = 'Greeley' THEN 'Greeley'
	 WHEN UPPER(TRIM(city)) IN ( 'HIGHLANDS RANCH', 'HighlandsRanch') THEN 'Highlands Ranch'
	 WHEN UPPER(TRIM(city)) = 'Lakewood' THEN 'Lakewood'
	 WHEN UPPER(TRIM(city)) = 'Littleton' THEN 'Littleton'
	 WHEN UPPER(TRIM(city)) = 'longmont' THEN 'Longmont'
	 WHEN UPPER(TRIM(city)) = 'Loveland' THEN 'Loveland'
	 WHEN UPPER(TRIM(city)) = 'Pueblo' THEN 'Pueblo'
	 WHEN UPPER(TRIM(city)) = 'thornton' THEN 'Thornton'
	 WHEN UPPER(TRIM(city)) = 'westminster' THEN 'Westminster'
	 WHEN city IS NULL THEN 'n/a'
	 ELSE TRIM(city)
END AS city,
CASE WHEN num_customers LIKE '%[^0-9]%' THEN '0'
	 WHEN num_customers IS NULL THEN '0'
	 ELSE TRIM(num_customers)
END AS num_customers,
CASE WHEN manager_name LIKE 'Mgr%' THEN manager_name
	 ELSE 'n/a'
END AS manager_name
FROM bronze.erp_branches;
