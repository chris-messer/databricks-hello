# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

race_results_df = spark.read.table("f1_presentation.race_results")

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

driver_standings = (race_results_df
                    .groupBy('race_year','driver_name','driver_nationality','team')
                    .agg(
                        sum('points').alias('total_points'),
                        count(when(col('position') == 1, True)).alias('wins')
                         )
)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

final_df.write\
    .mode('overwrite')\
    .saveAsTable('f1_presentation.driver_standings')
