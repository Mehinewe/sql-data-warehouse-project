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
