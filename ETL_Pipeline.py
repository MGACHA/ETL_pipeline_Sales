
#ETL Pipeline: Kaggle - Pandas - SQL Server

#  1. Extract   – authenticate, download dataset from Kaggle and read into DataFrame
#  2. Transform – clean and standardise the data
#  3. Load      – push the result to SQL Server via SQLAlchemy

import sys
import os
import pandas as pd
from sqlalchemy import create_engine
import kaggle

# CONFIGURATION DETAILS  

KAGGLE_DATASET  = "vivek468/superstore-dataset-final"
DOWNLOAD_PATH   = "data" # folder where the CSV will be saved in repo
CSV_FILE        = os.path.join(DOWNLOAD_PATH, "Sample - Superstore.csv") #name of the file form kaggle
CSV_ENCODING    = "latin1"

SQL_SERVER      = "servername"
SQL_DATABASE    = "etl_project"
SQL_TABLE       = "Superstore_clean_data"
ODBC_DRIVER     = "ODBC+Driver+17+for+SQL+Server"

# STEP 1 – EXTRACT

def extract() -> pd.DataFrame:
    r"""Authenticate with Kaggle API KEY saved in C:\Users\username\.kaggle, download the dataset and read it into a DataFrame."""
    print("\n[1/3] Extracting data from Kaggle...")

    # Authenticate & download
    kaggle.api.authenticate()

    os.makedirs(DOWNLOAD_PATH, exist_ok=True)

    kaggle.api.dataset_download_files(
        KAGGLE_DATASET,
        path=DOWNLOAD_PATH,
        unzip=True
    )

    # Optional: save dataset metadata alongside the data
    kaggle.api.dataset_metadata(KAGGLE_DATASET, path=DOWNLOAD_PATH)

    print(f" Dataset downloaded and extracted to {DOWNLOAD_PATH}/'")

    # Read into DataFrame
    if not os.path.exists(CSV_FILE):
        print(f" File not found: {CSV_FILE}")
        sys.exit(1)

    df = pd.read_csv(CSV_FILE, encoding=CSV_ENCODING)

    print(f" Loaded {len(df):,} rows × {len(df.columns)} columns")
    print(df.head())
    
    # -----------------------
    # INSPECT DATASET
    # -----------------------
    print("\nDATA INSPECTION")
    print(f"Rows: {df.shape[0]}, Columns: {df.shape[1]}")
    print("\nData types:")
    print(df.dtypes)
    print("\nMissing values:")
    print(df.isnull().sum())
    print("\nSummary statistics:")
    print(df.describe())

    # Data inspection summary
    df.describe().to_csv("outputs/summary_statistics.csv")
   

    # -----------------------
    # DATA QUALITY CHECKS
    # -----------------------
    print("\nDATA QUALITY CHECKS")
    print("Negative sales:", (df['Sales'] < 0).sum())
    print("Negative profit:", (df['Profit'] < 0).sum())
    print("Zero sales:", (df['Sales'] == 0).sum())

    return df

# STEP 2 – TRANSFORM - data checking

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardise the DataFrame."""
    print("\n[2/3] Transforming data...")

    # Remove null values
    before = len(df)
    df = df.dropna()
    print(f" Dropped nulls: {before - len(df):,} rows removed")

    # Remove duplicate rows
    before = len(df)
    df = df.drop_duplicates()
    print(f" Dropped duplicates: {before - len(df):,} rows removed")

    # Standardise column names (lowercase + underscores)
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    # Parse any date columns to datetime dd/mm/yyyy
    date_cols = [col for col in df.columns if "date" in col.lower()]
    for col in date_cols:
        print(f" Parsing dates: {col}")
        df[col] = pd.to_datetime(df[col], format="%m/%d/%Y", errors="coerce")

    # Extract Year, Month, Day from the order date
    df['order_year'] = df['order_date'].dt.year
    df['order_month'] = df['order_date'].dt.month
    df['order_month_name'] = df['order_date'].dt.month_name()

    # Calculate delivery days KPI
    df['delivery_days'] = (df['ship_date'] - df['order_date']).dt.days

    # Profit margin
    df['profit_margin'] = df['profit'] / df['sales']

    # Sales category - segmentation
    df['sales_category'] = pd.cut(
        df['sales'],
        bins=[0, 100, 500, 1000, 10000],
        labels=['Low', 'Medium', 'High', 'Very High']
        )

    print(f"\n Transform complete – {len(df):,} rows remaining")
    print(df.info())

    # -----------------------
    # KPI OUTPUTS
    # -----------------------
    print("\nKPI SUMMARY")

    # Monthly sales
    monthly_sales = df.groupby(['order_year', 'order_month'])['sales'].sum().reset_index()
    print("\nMonthly sales (sample):")
    print(monthly_sales.head())

    # Average delivery time
    print("\nAverage delivery days:", df['delivery_days'].mean())

    # Profit by category
    profit_by_category = df.groupby('category')['profit'].sum()
    print("\nProfit by category:")
    print(profit_by_category)

    # Monthly sales KPI
    monthly_sales.to_csv("outputs/monthly_sales.csv", index=False)


    return df




# STEP 3 – LOAD

def load(df: pd.DataFrame) -> None:
    """Write the cleaned DataFrame to SQL Server."""
    print("\n[3/3] Loading data into SQL Server...")

    connection_string = (
        f"mssql+pyodbc://@{SQL_SERVER}/{SQL_DATABASE}"
        f"?driver={ODBC_DRIVER}"
    )

    engine = create_engine(connection_string)

    df.to_sql(SQL_TABLE, engine, if_exists="replace", index=False)

    print(f" Data loaded successfully into [{SQL_DATABASE}].[dbo].[{SQL_TABLE}]")


# MAIN

def main() -> None:
    print("=" * 50)
    print("ETL PIPELINE  –  Superstore Dataset")
    print("=" * 50)

    df = extract()
    df = transform(df)
    load(df)

    print("\n Pipeline finished successfully.")


if __name__ == "__main__":
    main()

