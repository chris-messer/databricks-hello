# Databricks notebook source
# MAGIC %md
# MAGIC ## Step 1. Import results JSON

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, FloatType
dbutils.widgets.text('p_data_source','')
data_source = dbutils.widgets.get('p_data_source')


# COMMAND ----------

results_schema = StructType(fields=[
    StructField('resultId',IntegerType(), False),
    StructField('raceId',IntegerType(), False),
    StructField('driverId',IntegerType(), False),
    StructField('constructorId',IntegerType(), False),
    StructField('number',IntegerType(), True),
    StructField('grid',IntegerType(), False),
    StructField('position',IntegerType(), True),
    StructField('positionText',StringType(), False),
    StructField('positionOrder',IntegerType(), False),
    StructField('points',FloatType(), False),
    StructField('laps',IntegerType(), False),
    StructField('time',StringType(), True),
    StructField('milliseconds',IntegerType(), True),
    StructField('fastestLap',IntegerType(), True),
    StructField('rank',IntegerType(), True),
    StructField('fastestLapTime',StringType(), True),
    StructField('fastestLapSpeed',StringType(), True),
    StructField('statusId',IntegerType(), False)
    ])

# COMMAND ----------

results_df = spark.read\
    .schema(results_schema)\
    .json('/mnt/f1dbhello/raw/results.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 2 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

results_final_df = results_df\
    .withColumnRenamed('resultsId','results_id')\
    .withColumnRenamed('raceId','race_id')\
    .withColumnRenamed('driverId','driver_id')\
    .withColumnRenamed('constructorId','constructor_id')\
    .withColumnRenamed('positionText','position_text')\
    .withColumnRenamed('positionOrder','position_order')\
    .withColumnRenamed('fastestLap','fastest_lap')\
    .withColumnRenamed('fastestLapTime','fastest_lap_time')\
    .withColumnRenamed('fastestLapSpeed','fastest_lap_speed')\
    .withColumn('ingestion_date', current_timestamp())\
    .withColumn('data_source',lit(data_source))\
    .drop(col('statusId'))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Save to parquet

# COMMAND ----------

results_final_df.write\
    .mode('overwrite')\
    .partitionBy('race_id')\
    .parquet('/mnt/f1dbhello/processed/results')

# COMMAND ----------

# %fs
# ls /mnt/f1dbhello/processed/results

display(spark.read.parquet('/mnt/f1dbhello/processed/results'))

# COMMAND ----------

dbutils.notebook.exit('Success')
