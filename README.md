# Data Warehouse & Analytics Project

Welcome to my Data Warehouse and Analytics Project 🚀
In this project, I built a complete data solution — from raw data to interactive insights.
---
## 🏗️ Data Architecture

This project follows the Medallion Architecture with three layers: **Bronze**, **Silver**, and **Gold**:
<img width="841" height="531" alt="Data Architecture  drawio" src="https://github.com/user-attachments/assets/381c87d3-c62e-4a8f-8511-581063b0a8a4" />

1. **Bronze Layer**: Stores raw data exactly as received. Data is loaded from CSV files into SQL Server Database.
2. **Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer**: Houses business-ready data modeled into a star schema,  Ready for  analytics, reporting and dashboards.

---
## 📖 Project Overview

This project involves:

1. **Data Architecture**: Designing a Modern Data Warehouse Using Medallion Architecture **Bronze**, **Silver**, and **Gold** layers.
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights.

🎯 Skills Demonstrated
This project highlights my ability in:
- SQL Development
- Data Warehousing
- ETL Pipeline Development
- Data Modeling (Star Schema)
- Data Analytics & Reporting 

---

## 🛠️ Tools:
- **SQL Server**
- **Draw.io:** to Design data architecture, models, flows, and diagrams.
- **Notion:** to plan the project phases and tasks
- **Github:** to manage versions, and collaborate  efficiently.

---

## 🚀 Project Requirements

### Building the Data Warehouse (Data Engineering)

#### Objective
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

#### Specifications
- **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis.
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope**: Focus on the latest dataset only; historization of data is not required.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

---

### BI: Analytics & Reporting (Data Analysis)

#### Objective
Develop SQL-based analytics to deliver detailed insights into:
- **Customer Behavior**
- **Product Performance**
- **Sales Trends**

These insights empower stakeholders with key business metrics, enabling strategic decision-making.  


## 📂 Repository Structure
```
data-warehouse-project/
│
├── datasets/                           # Raw datasets used for the project (ERP and CRM data)
│
├── docs/                               # Project documentation and architecture details
│   ├── data_architecture.drawio        # Draw.io file shows the project's architecture
│   ├── data_catalog.md                 # Catalog of datasets, including field descriptions and metadata
│   ├── data_flow.drawio                # Draw.io file for the data flow diagram
│   ├── data_models.drawio              # Draw.io file for data models (star schema)
│   ├── naming-conventions.md           # Consistent naming guidelines for tables, columns, and files
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── bronze/                         # Scripts for extracting and loading raw data
│   ├── silver/                         # Scripts for cleaning and transforming data
│   ├── gold/                           # Scripts for creating analytical models
│
├── tests/                              # Test scripts and quality files
│
├── README.md                           # Project overview and instructions
└── LICENSE                             # License information for the repository  
```
---

## 🛡️ License

This project is licensed under the [MIT License](LICENSE). Feel free to use, modify, and share it with proper attribution.

## 🗒️ Data Source & Attribution
**Data Source**: Huge thank you to [Baraa Khatib Salkini](https://www.linkedin.com/in/baraa-khatib-salkini/) for breaking down complex concepts and making data learning practical and accessible.  
**Note**: All analysis, data modeling, and visualizations are my own work.

## 🌟 About Me
Hi there, I’m Mehinewe Kedewouli 👋  
I’m a Data Analyst, and I help organizations leverage their data to accelerate performance and growth.

Let's stay in touch! Feel free to connect with me on the following platforms:

[![Youtube](https://img.shields.io/badge/YouTube-red?style=for-the-badge&logo=youtube&logoColor=white)](https://www.youtube.com/@LeverageDataa)
[![Linkedin](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/mehinewe-kedewouli/)

