# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuits.csv file 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - read the CSV file

# COMMAND ----------

circuits_df = spark.read.csv('dbfs:/mnt/f1dbhello/raw/circuits.csv', header = True)

# COMMAND ----------

display(circuits_df)
