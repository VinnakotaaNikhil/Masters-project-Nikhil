# Databricks notebook source
# ============================================================
# CREATE ACCOUNT POOL MASTER TABLE
# ============================================================

import random
from pyspark.sql import types as T

ACCOUNT_COUNT = 1000  # number of unique accounts
MIN_BAL = 200.0
MAX_BAL = 5000.0

accounts = []
seen = set()
for _ in range(ACCOUNT_COUNT):
    acc_no = random.randint(10_000_000, 99_999_999)
    while acc_no in seen:
        acc_no = random.randint(10_000_000, 99_999_999)
    seen.add(acc_no)
    start_balance = round(random.uniform(MIN_BAL, MAX_BAL), 2)
    accounts.append(acc_no)

# Assign usage weights (some accounts more active)
weights = []
for acc in accounts:
    if random.random() < 0.2:
        w = random.uniform(5, 10)  # heavy users
    else:
        w = random.uniform(1, 3)   # light users
    weights.append((acc, round(random.uniform(MIN_BAL, MAX_BAL), 2), w))

schema = T.StructType([
    T.StructField("Account_Number", T.LongType(), False),
    T.StructField("Current_Balance", T.DoubleType(), False),
    T.StructField("Usage_Weight", T.DoubleType(), False)
])

df_accounts = spark.createDataFrame(weights, schema)
df_accounts.write.mode("overwrite").format("delta").saveAsTable("account_pool_master")

print("account_pool_master created successfully!")
display(df_accounts.limit(10))