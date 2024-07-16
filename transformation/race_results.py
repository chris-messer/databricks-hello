# Databricks notebook source
# MAGIC %md
# MAGIC # Create Race Results Gold Table

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Read files

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------



races_df = spark.read.table('f1_processed.races')\
    .withColumnRenamed('name','race_name')\
    .withColumnRenamed('race_timestamp','race_date')

circuits_df = spark.read.table(f'f1_processed.circuits')\
    .withColumnRenamed('location','circuit_location')

drivers_df = spark.read.table(f'f1_processed.drivers')\
    .withColumnRenamed('name','driver_name')\
    .withColumnRenamed('number','driver_number')\
    .withColumnRenamed('nationality','driver_nationality')

constructors_df = spark.read.table(f'f1_processed.constructors')\
    .withColumnRenamed('name','team')
    
results_df = spark.read.table(f'f1_processed.results')\
    .withColumnRenamed('time','race_time')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Join Tables

# COMMAND ----------


race_results_df = (races_df
              .join(circuits_df,races_df.circuit_id == circuits_df.circuit_id, 'inner')
              .join(results_df, results_df.race_id == races_df.race_id, 'inner')
              .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id,'inner')
              .join(drivers_df, results_df.driver_id == drivers_df.driver_id,'inner')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 Select Columns

# COMMAND ----------

race_results_selected_df = (race_results_df
                   .select(race_results_df.race_year,
                           race_results_df.race_name,
                           race_results_df.race_date,
                           race_results_df.circuit_location,
                           race_results_df.driver_name,
                           race_results_df.driver_number,
                           race_results_df.driver_nationality,
                           race_results_df.team,
                           race_results_df.grid,
                           race_results_df.fastest_lap,
                           race_results_df.race_time,
                           race_results_df.points,
                           race_results_df.position
                           )
                   .withColumn('created_date', current_timestamp())
                   
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Save to Gold Layer

# COMMAND ----------

(race_results_selected_df.write
 .mode('overwrite')
 .saveAsTable('f1_presentation.race_results'))
