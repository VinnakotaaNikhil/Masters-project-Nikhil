# Databricks notebook source
# DBTITLE 1,Connection to ADLS
# ============================================================
# CONNECT AZURE DATABRICKS TO ADLS GEN2 (USING STORAGE KEY)
# ============================================================

# Replace with your real values
account_name = "stgsyntheticdata"        # storage account name
container_name = "cleansed"                   # your ADLS container name
account_key = "qbSJum0asirUIMUzS1AZgyCDwEVqGiU8DJSik7YR3bOfDxOviOfs7ZcZnhkILaIvX+BlMFdL4BvQ+ASt6IyXlw=="
 # from Azure Portal -> Access keys

# Configure Spark for ADLS access
spark.conf.set(f"fs.azure.account.key.{account_name}.dfs.core.windows.net", account_key)

# Helper function for easy ABFSS path building
def abfss(path):
    return f"abfss://{container_name}@{account_name}.dfs.core.windows.net{path}"

print("Connected to ADLS Gen2 successfully!")
print("Example path:", abfss("/"))

# COMMAND ----------

# ============================================================
# NOTEBOOK 2: BRONZE CSV → SILVER (Timestamp Based Partitioning)
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import types as T

# ----------------------------
# CONFIG
# ----------------------------
RAW_BASE_PATH     = f"abfss://raw@{account_name}.dfs.core.windows.net"
SILVER_CSV_PATH   = f"abfss://cleansed@{account_name}.dfs.core.windows.net"

# ----------------------------------------------------------
# 1. READ ALL RAW CSV FILES (all folders)
# ----------------------------------------------------------
bronze_df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{RAW_BASE_PATH}/*/*.csv")
)

# ----------------------------------------------------------
# 2. CAST TIMESTAMP + BASIC TYPES
# ----------------------------------------------------------
df = bronze_df

# parse ISO timestamp with Z (correct)
df = df.withColumn(
    "Timestamp",
    F.to_timestamp("Timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
)

# force timestamp to UTC
df = df.withColumn(
    "Timestamp",
    F.to_utc_timestamp("Timestamp", "UTC")
)

# cast numeric types
df = (
    df.withColumn("Account_Number", F.col("Account_Number").cast("long"))
      .withColumn("Opening_Balance", F.col("Opening_Balance").cast("double"))
      .withColumn("Amount", F.col("Amount").cast("double"))
      .withColumn("Closing_Balance", F.col("Closing_Balance").cast("double"))
)

# ----------------------------------------------------------
# 3. CLEAN STRING COLUMNS
# ----------------------------------------------------------
string_cols = [c for c, t in df.dtypes if t == "string"]

for c in string_cols:
    df = df.withColumn(c, F.trim(F.col(c)))

df = df.fillna({c: "N/A" for c in string_cols})

# ----------------------------------------------------------
# 4. STANDARDISE CASE
# ----------------------------------------------------------
def initcap_safe(col): return F.when(F.col(col) != "N/A", F.initcap(F.col(col))).otherwise("N/A")
def upper_safe(col):   return F.when(F.col(col) != "N/A", F.upper(F.col(col))).otherwise("N/A")

df = df.withColumn("Merchant_Name", initcap_safe("Merchant_Name"))
df = df.withColumn("Location", initcap_safe("Location"))
df = df.withColumn("Country", upper_safe("Country"))
df = df.withColumn("Status", upper_safe("Status"))
df = df.withColumn("Channel", upper_safe("Channel"))
df = df.withColumn("Transaction_Mode", upper_safe("Transaction_Mode"))
df = df.withColumn("Currency", upper_safe("Currency"))
df = df.withColumn("Type", initcap_safe("Type"))
df = df.withColumn("Category", upper_safe("Category"))

# ----------------------------------------------------------
# 5. SPLIT DATE & TIME
# ----------------------------------------------------------
df = df.withColumn("Date", F.to_date("Timestamp"))
df = df.withColumn("Time", F.date_format("Timestamp", "HH:mm:ss"))

# ----------------------------------------------------------
# 6. CATEGORY STANDARDISATION
# ----------------------------------------------------------
category_map = {
    "GROCERIES": "Groceries",
    "FOOD": "Food",
    "TRAVEL": "Travel",
    "FUEL": "Fuel",
    "PHARMACY": "Pharmacy",
    "DIGITAL": "Digital",
    "ENTERTAINMENT": "Entertainment",
    "SHOPPING": "Shopping",
    "ELECTRONICS": "Electronics",
    "CLOTHING": "Clothing",
    "SALARY": "Salary",
    "BILLS": "Bills",
    "CASH WITHDRAWAL": "Cash Withdrawal",
}

for src, tgt in category_map.items():
    df = df.withColumn(
        "Category",
        F.when(F.col("Category") == src, tgt).otherwise(F.col("Category"))
    )

df = df.withColumn(
    "Category",
    F.when(
        (~F.col("Category").isin(list(category_map.values()))) & (F.col("Category") != "N/A"),
        "Other"
    ).otherwise(F.col("Category"))
)

# ----------------------------------------------------------
# 7. ROUND BALANCES TO 2 DECIMALS
# ----------------------------------------------------------
df = df.withColumn("Opening_Balance", F.round(F.col("Opening_Balance"), 2))
df = df.withColumn("Closing_Balance", F.round(F.col("Closing_Balance"), 2))
df = df.withColumn("Amount", F.round(F.col("Amount"), 2))

# ----------------------------------------------------------
# 8. REMOVE DUPLICATES
# ----------------------------------------------------------
df = df.dropDuplicates(["Transaction_ID"])

# ----------------------------------------------------------
# 9. COMPUTE YEAR_MONTH PARTITION
# ----------------------------------------------------------
df = df.withColumn("year_month", F.date_format("Timestamp", "yyyy_MM"))

months = df.select("year_month").distinct().collect()

print("Detected months →", months)

# ----------------------------------------------------------
# 10. WRITE SEPARATE SILVER TABLE + CSV FOR EACH MONTH
# ----------------------------------------------------------
for ym_row in months:
    ym = ym_row["year_month"]
    year, month = ym.split("_")
    table_name = f"silver_{year}_{month}"
    
    print(f"\nWriting table: {table_name}")
    
    df_month = df.filter(F.col("year_month") == ym)

    # Write Delta table
    (
        df_month.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(table_name)
    )

    # Write single CSV file
    (
        df_month.coalesce(1)
            .write
            .option("header", "true")
            .mode("overwrite")
            .csv(f"{SILVER_CSV_PATH}/{ym}")
    )

    print(f"Table saved: {table_name}")
    print(f"CSV saved: {SILVER_CSV_PATH}/{ym}")

print("\nSilver processing complete.")