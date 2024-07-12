# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1. Ingest drivers JSON file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
dbutils.widgets.text('p_data_source','')
data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

name_schema = StructType(fields=[StructField('forename', StringType(), False),
                                   StructField('surname', StringType(), False)])

driver_schema = StructType(fields=[StructField('driverId', IntegerType(), False),
                                   StructField('driverRef', StringType(), False),
                                   StructField('number', StringType(), False),
                                   StructField('code', StringType(), False),
                                   StructField('name', name_schema),
                                   StructField('dob', DateType(), False),
                                   StructField('nationality', StringType(), False),
                                   StructField('url', StringType(), False)])


# COMMAND ----------

drivers_df = spark.read\
    .schema(driver_schema)\
    .json('/mnt/f1dbhello/raw/drivers.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Rename and add columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, col, lit

drivers_selected_df = drivers_df\
    .withColumnRenamed('driverId','driver_id')\
    .withColumnRenamed('driverRef','driver_ref')\
    .withColumn('ingestion_date',current_timestamp())\
    .withColumn('name',
        concat(
            col('name.forename'),
            lit(' '),
            col('name.surname')
            ))\
    .withColumn('data_source',lit(data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 3 - Drop Columns

# COMMAND ----------

drivers_final_df = drivers_selected_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Write to parquet

# COMMAND ----------

drivers_final_df.write\
    .mode('overwrite')\
    .parquet('/mnt/f1dbhello/processed/drivers')

# COMMAND ----------

dbutils.notebook.exit('Success')
