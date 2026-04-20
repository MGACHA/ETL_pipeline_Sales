# ETL_pipeline_Sales

## Sales ETL Pipeline (Python + SQL Server)

### Overview

This project builds an end-to-end ETL pipeline using a retail dataset. It extracts raw data, cleans and transforms it, calculates business KPIs, and loads the results into SQL Server for analysis.

### What I did:

1. Extracted dataset from Kaggle using API
2. Performed data inspection (schema, missing values, distributions)
3. Implemented data quality checks:
- negative sales and profit
- zero-value transactions
4. Cleaned and standardised the dataset
5. Converted date columns to proper datetime format
6. Created business KPIs:
- delivery time (from order to ship)
- profit margin
- monthly sales trends
7. Loaded clean dataset into SQL Server
8. Data quality checks and KPI outputs are saved in the /outputs and /images folder for reproducibility:
  - [Monthly Sales](outputs/monthly_sales.cvs)
  - [Summary](outputs/summary_statistics.cvs)
