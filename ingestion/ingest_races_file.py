# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest races.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Read the CSV

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

# COMMAND ----------

races_schema = StructType(fields=[StructField('raceId', IntegerType(), False),
                                  StructField('year', IntegerType(), False),
                                  StructField('round', IntegerType(), False),
                                  StructField('circuitId', IntegerType(), False),
                                  StructField('name', StringType(), False),
                                  StructField('date', StringType(), False),
                                  StructField('time', StringType(), False),
                                  StructField('url', StringType(), False)])

# COMMAND ----------

races_df = spark.read\
    .option('header', True)\
    .schema(races_schema)\
    .csv('/mnt/f1dbhello/raw/races.csv')    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Select columns

# COMMAND ----------

from pyspark.sql.functions import col

races_selected_df = races_df.select(
    col('raceId'),
    col('year'),
    col('round'),
    col('circuitId'),
    col('name'),
    col('date'),
    col('time'),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Rename Columns

# COMMAND ----------

races_renamed_df = races_selected_df\
    .withColumnRenamed('raceId','race_id') \
    .withColumnRenamed('year','race_year') \
    .withColumnRenamed('circuitId','circuit_id')


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Transform Columns

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, lit

races_transformed_df = races_renamed_df\
    .withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))


# COMMAND ----------

from pyspark.sql.functions import current_timestamp
races_df_final = races_transformed_df\
    .select(
    col('race_id'),
    col('race_year'),
    col('round'),
    col('circuit_id'),
    col('name'),
    col('race_timestamp')
    )\
    .withColumn('ingestion_date',current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Save to datalake Silver Layer

# COMMAND ----------

races_df_final.write \
    .mode('overwrite') \
    .partitionBy('race_year') \
    .parquet('/mnt/f1dbhello/processed/races')

# COMMAND ----------

display(spark.read.parquet('/mnt/f1dbhello/processed/races'))
