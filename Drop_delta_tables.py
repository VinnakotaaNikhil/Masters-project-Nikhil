# Databricks notebook source
# DBTITLE 1,Drop all delta tables
for t in spark.catalog.listTables("default"):
    if t.tableType.lower() == "managed" or t.provider == "delta":
        spark.sql(f"DROP TABLE IF EXISTS default.{t.name}")
        print(f" Dropped Delta table: {t.name}")

print("All Delta-format tables in 'default' database dropped.")
