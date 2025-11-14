# 🏦 Colorado Bank Analytics Project  

## 👋 Welcome!  
Welcome to my **Colorado Bank Analytics** project.  
This end-to-end data analytics case study explores customer behavior, product performance, and transaction patterns for a fictional Colorado-based bank.  

The dataset contains messy, realistic banking data across customers, accounts, transactions, products, and branches.  
As the data analyst, my goal is to clean, model, and analyze this data to help the bank’s leadership make informed decisions about customer retention, profitability, fraud detection, and product strategy.  

---

## 🧩 Project Requirements  

### 🎯 Objective  
Build a full analytics solution that transforms messy raw data into clear insights using SQL and Power BI.  
This includes:  
- Cleaning and transforming data (ETL).  
- Modeling relationships between banking entities.  
- Delivering business insights through dashboards and reports.  

### 🧾 Specification  

#### 🗂️ Data Source  
The data is from three source systems ( ERP, CBS and CRM)

- **customers_crm.csv** – Customer profiles and demographics  
- **accounts_cbs.csv** – Account details, balances, credit limits  
- **transactions_cbs.csv** – Transaction history and channels  
- **products_cbs.csv** – Financial products and status  
- **branches_erp.csv** – Branch performance across Colorado  

> *All datasets are synthetic but modeled on real-world banking structures.*  

#### 🧹 Data Quality  
We have to clean and resolve data quality issues prior to analysis.  

#### 🔗 Integrations  
The cleaned datasets are integrated using SQL joins across entities:  

```
customers (1) ───< accounts
customers (1) ───< transactions
customers (1) ───< products
branches  (1) ───< accounts
```

#### 🧭 Scope  
The analysis focuses on:  
- Customer retention and churn patterns  
- Branch profitability and city-level performance  
- Fraud and anomaly detection  
- Credit risk exposure  
- Product usage and cross-sell opportunities  

#### 📚 Documentation  
Key deliverables include:  
- **SQL scripts** for cleaning, transformation, and aggregation  
- **Python notebooks** for exploratory data analysis (EDA)  
- **Power BI dashboard** showing KPIs, trends, and insights  
- **Business recommendations report** summarizing findings  

---

## 📊 BI: Analytics & Reporting (Data Analytics)

### 🎯 Objective  
Develop SQL-based analytics and Power BI dashboards to deliver detailed insights into:  
- **Customer Behavior**  
- **Product Performance**  
- **Transaction Trends**  

These insights empower stakeholders with key business metrics, enabling **strategic decision-making**, risk control, and targeted marketing.  

---

## 📜 Licence  
This project is licensed under the [MIT Licence](LICENSE).  
You are free to use, modify, and share this project with proper attribution.  

---

## 👩‍💻 About Me  

Hi there, I’m **Mehinewe Kedewouli** 👋  
I’m a **data analyst** who enjoys turning messy data into meaningful insights.  
I work with tools like **SQL**, **Excel**, and **Power BI** to clean, analyze, and present data clearly.  
I’m passionate about using data to solve real-world problems and help organizations make smarter, evidence-based decisions.  
