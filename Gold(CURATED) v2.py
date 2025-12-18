# Databricks notebook source
# ============================================================
# CONNECT AZURE DATABRICKS TO ADLS GEN2 (USING STORAGE KEY)
# ============================================================

# Replace with your real values
account_name = "stgsyntheticdata"        # storage account name
container_name = "curated"                   # your ADLS container name
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
# NOTEBOOK 3: SILVER (CLEANSED CSV) → CURATED (Fraud + Monitor v3)
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ----------------------------
# CONFIG
# ----------------------------
account_name = "stgsyntheticdata"  # <-- set this once
SILVER_CSV_PATH = f"abfss://cleansed@{account_name}.dfs.core.windows.net"
CURATED_PATH    = f"abfss://curated@{account_name}.dfs.core.windows.net"

# ----------------------------
# 1. READ SILVER CSV
# ----------------------------
df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{SILVER_CSV_PATH}/*/*.csv")
)

# ----------------------------
# CAST TYPES AND CLEAN BASE FIELDS
# ----------------------------
df = df.withColumn("Timestamp", F.to_timestamp("Timestamp"))
df = df.filter(F.col("Timestamp").isNotNull())

df = df.withColumn("Status",  F.upper(F.col("Status")))
df = df.withColumn("Country", F.upper(F.col("Country")))
df = df.withColumn("Amount",  F.col("Amount").cast("double"))
df = df.withColumn("Account_Number", F.col("Account_Number").cast("long"))

df = df.withColumn("year_month", F.date_format("Timestamp", "yyyy_MM"))

months = [r["year_month"] for r in df.select("year_month").distinct().collect()]
print("Detected months:", months)

# ============================================================
# PROCESS EACH MONTH
# ============================================================
for ym in months:
    print(f"\n Processing month {ym}")
    year, month = ym.split("_")
    table_name = f"curated_{year}_{month}"

    dfx = df.filter(F.col("year_month") == ym)

    # ----------------------------
    # WINDOW FUNCTIONS
    # ----------------------------
    w = Window.partitionBy("Account_Number").orderBy("Timestamp")
    ts_long = F.col("Timestamp").cast("long")

    dfx = (
        dfx
        .withColumn("Prev_Timestamp", F.lag("Timestamp").over(w))
        .withColumn("Next_Timestamp", F.lead("Timestamp").over(w))
        .withColumn("Prev_Status",    F.lag("Status").over(w))
        .withColumn("Next_Status",    F.lead("Status").over(w))
        .withColumn("Prev_Country",   F.lag("Country").over(w))
        .withColumn("Next_Country",   F.lead("Country").over(w))
    )

    dfx = dfx.withColumn(
        "Prev_Delta_Min",
        (F.unix_timestamp("Timestamp") - F.unix_timestamp("Prev_Timestamp")) / 60.0
    ).withColumn(
        "Next_Delta_Min",
        (F.unix_timestamp("Next_Timestamp") - F.unix_timestamp("Timestamp")) / 60.0
    )

    # ============================================================
    # FRAUD PATTERNS
    # ============================================================

    # 1) Repeated declines (3+ in 10 min)
    is_decline = (F.col("Status") == "DECLINED")

    w_10min = (
        Window.partitionBy("Account_Number")
              .orderBy(ts_long)
              .rangeBetween(-10 * 60, 0)
    )

    dfx = dfx.withColumn(
        "Decline_Count_10min",
        F.sum(F.when(is_decline, 1).otherwise(0)).over(w_10min)
    )

    pat_repeated_declines = is_decline & (F.col("Decline_Count_10min") >= 3)

    # 2) Foreign burst (>=2 foreign in 45 min)
    foreign_flag = F.when(F.col("Country") != "UK", 1).otherwise(0)

    w_45min = (
        Window.partitionBy("Account_Number")
              .orderBy(ts_long)
              .rangeBetween(-45 * 60, 0)
    )

    dfx = dfx.withColumn(
        "Foreign_Count_45min",
        F.sum(foreign_flag).over(w_45min)
    )

    pat_foreign_burst = (
        (F.col("Country") != "UK") &
        (F.col("Foreign_Count_45min") >= 2)
    )

    # 3) High amount (>= 2000)
    pat_high_amount = (F.col("Amount") >= 2000)

    # 4) International decline
    pat_international_decline = (
        (F.col("Country") != "UK") &
        (F.col("Status") == "DECLINED")
    )

    # 5) Impossible travel
    pat_impossible_travel = (
        (F.col("Prev_Country") == "UK") &
        (F.col("Country") != "UK") &
        (F.col("Prev_Delta_Min") <= 30)
    )

    # ============================================================
    # MONITOR PATTERNS
    # ============================================================

    # 6) Back-to-back Declined → Completed (or reverse)
    pat_back_to_back = (
        ((F.col("Status") == "DECLINED") &
         (F.col("Next_Status") == "COMPLETED") &
         (F.col("Next_Delta_Min") <= 10))
        |
        ((F.col("Status") == "COMPLETED") &
         (F.col("Prev_Status") == "DECLINED") &
         (F.col("Prev_Delta_Min") <= 10))
    )

    # 7) Night high (00–05 and amount ≥ 1000)
    hour_col = F.hour("Timestamp")
    pat_night_high = (hour_col <= 5) & (F.col("Amount") >= 1000)

    # 8) High velocity (4–7 txns in 5 min)
    w_5min = (
        Window.partitionBy("Account_Number")
              .orderBy(ts_long)
              .rangeBetween(-5 * 60, 0)
    )

    dfx = dfx.withColumn("Txn_Count_5min", F.count("*").over(w_5min))

    pat_high_velocity = (
        (dfx["Txn_Count_5min"] >= 4) &
        (dfx["Txn_Count_5min"] <= 7) &
        (F.col("Type") != "Deposit")
    )

    # 9) Generic foreign activity (new fallback monitor rule)
    is_foreign_country = (
        (F.col("Country") != "UK") &
        (F.col("Country").isNotNull()) &
        (F.col("Country") != "N/A")
    )

    pat_foreign_activity = is_foreign_country

    # ============================================================
    # CLASSIFICATION — FRAUD
    # ============================================================

    is_fraud = (
        pat_repeated_declines |
        pat_foreign_burst |
        pat_high_amount |
        pat_international_decline |
        pat_impossible_travel
    )

    fraud_type = (
        F.when(pat_repeated_declines,     "repeated_declines")
         .when(pat_foreign_burst,         "foreign_burst")
         .when(pat_high_amount,           "high_amount")
         .when(pat_international_decline, "international_decline")
         .when(pat_impossible_travel,     "impossible_travel")
         .otherwise("")
    )

    # dfx = dfx.withColumn("Is_Fraud",   is_fraud.cast("boolean"))
    dfx = dfx.withColumn(
    "Is_Fraud",
    F.coalesce(is_fraud.cast("boolean"), F.lit(False))
    )
    dfx = dfx.withColumn("Fraud_Type", fraud_type)

    # ============================================================
    # CLASSIFICATION — MONITOR
    # ============================================================
    monitor_type = (
        F.when(pat_back_to_back,        "back_to_back")
         .when(pat_night_high,          "night_high")
         .when(pat_high_velocity,       "high_velocity")
         .when(pat_impossible_travel,   "impossible_travel")
         # NEW RULE: Anything foreign that is NOT fraud becomes monitored
         .when(pat_foreign_activity & (~is_fraud), "foreign_activity")
         .otherwise("")
    )

    dfx = dfx.withColumn("Need_Monitor", (monitor_type != "").cast("boolean"))
    dfx = dfx.withColumn("Monitor_Type", monitor_type)

    # ============================================================
    # CLEANUP TEMP COLUMNS
    # ============================================================
    dfx = dfx.drop(
        "Prev_Timestamp","Next_Timestamp",
        "Prev_Status","Next_Status",
        "Prev_Country","Next_Country",
        "Prev_Delta_Min","Next_Delta_Min",
        "Decline_Count_10min","Foreign_Count_45min","Txn_Count_5min"
    )
    # -------------------------------------------
    # NEW RULE: Any foreign transaction must be fraud or monitor
    # -------------------------------------------

    dfx = dfx.withColumn(
        "Need_Monitor",
        F.when(
            (F.col("Country") != "UK") & (~F.col("Is_Fraud")),
            True
        ).otherwise(F.col("Need_Monitor"))
    )

    dfx = dfx.withColumn(
        "Monitor_Type",
        F.when(
            (F.col("Country") != "UK") & (~F.col("Is_Fraud")) & (F.col("Monitor_Type") == ""),
            "foreign_activity"
        ).otherwise(F.col("Monitor_Type"))
    )


    # ============================================================
    # WRITE OUTPUT
    # ============================================================
    print(f" Writing Delta table curated_{year}_{month}")
    (
        dfx.write
           .format("delta")
           .mode("overwrite")
           .saveAsTable(table_name)
    )

    print(f" Writing CSV → {CURATED_PATH}/{ym}")
    (
        dfx.coalesce(1)
           .write
           .option("header", "true")
           .mode("overwrite")
           .csv(f"{CURATED_PATH}/{ym}")
    )

print("\n CURATED v3 processing complete.")