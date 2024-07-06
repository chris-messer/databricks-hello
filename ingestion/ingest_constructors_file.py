# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Constructors file

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Ingest json file

# COMMAND ----------

constructor_schema = 'constructorId INTEGER, constructorRef STRING, name STRING, nationality STRING, url STRING'

# COMMAND ----------

constructor_df = spark.read\
    .schema(constructor_schema)\
    .json('/mnt/f1dbhello/raw/constructors.json')

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Transform File

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp
constructor_final_df = constructor_df\
    .drop(col('url'))\
    .withColumnRenamed('constructorId', 'constructor_id')\
    .withColumnRenamed('constructorRef','constructor_ref')\
    .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Load File

# COMMAND ----------

constructor_final_df.write\
    .mode('overwrite')\
    .parquet('/mnt/f1dbhello/processed/constructors')
