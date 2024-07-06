# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuits.csv file 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Read the CSV file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId",IntegerType(), False),
                                     StructField('circuitRef', StringType(), True),
                                     StructField('name', StringType(), True),
                                     StructField('location', StringType(), True), 
                                     StructField('country', StringType(), True), 
                                     StructField('lat', DoubleType(), True), 
                                     StructField('lng', DoubleType(), True), 
                                     StructField('alt', IntegerType(), True), 
                                     StructField('url', StringType(), True)])

# COMMAND ----------

circuits_df = spark.read\
.option("header", True)\
.schema(circuits_schema)\
.csv('dbfs:/mnt/f1dbhello/raw/circuits.csv', header = True)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Select only the required columns

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitRef","name")
# OR
circuits_selected_df = circuits_df.select(circuits_df.circuitRef, circuits_df.name)
# OR
circuits_selected_df = circuits_df.select(circuits_df['circuitRef'], circuits_df['name'])
# OR
from pyspark.sql.functions import col
circuits_selected_df = circuits_df.select(col('circuitRef'), col('name'))

# COMMAND ----------

circuits_selected_df = circuits_df.select(
 col("circuitId")
,col('circuitRef')
,col('name')
,col('location')
,col('country')
,col('lat')
,col('lng')
,col('alt')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Rename Columns

# COMMAND ----------

circuits_renamed_df = circuits_selected_df \
    .withColumnRenamed("circuitId","circuit_id") \
    .withColumnRenamed("circuitRef","circuit_ref") \
    .withColumnRenamed("lat","latitude") \
    .withColumnRenamed("lng","longitude") \
    .withColumnRenamed("alt","altitude") 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn('ingestion_date',current_timestamp()) \
    .withColumn('environment',lit('Production'))

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Write to datalake Silver Layer

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/f1dbhello/processed/circuits

# COMMAND ----------

circuits_final_df.write \
    .mode('overwrite') \
    .parquet('/mnt/f1dbhello/processed/circuits')
