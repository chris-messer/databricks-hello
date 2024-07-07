# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuits.csv file 

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text('p_data_source','')
data_source = dbutils.widgets.get('p_data_source')


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
.csv(f'{raw_folder_path}/circuits.csv', header = True)

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

from pyspark.sql.functions import lit

circuits_renamed_df = circuits_selected_df \
    .withColumnRenamed("circuitId","circuit_id") \
    .withColumnRenamed("circuitRef","circuit_ref") \
    .withColumnRenamed("lat","latitude") \
    .withColumnRenamed("lng","longitude") \
    .withColumnRenamed("alt","altitude") \
    .withColumn('data_source',lit(data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

circuits_final_df = circuits_renamed_df\
    .withColumn('environment',lit('Production'))

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Write to datalake Silver Layer

# COMMAND ----------

circuits_final_df.write \
    .mode('overwrite') \
    .parquet(f'{processed_folder_path}/circuits')

# COMMAND ----------

dbutils.notebook.exit('Success')
