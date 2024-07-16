# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_raw;

# COMMAND ----------

# MAGIC %run
# MAGIC ../includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest from CSV

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Ingest circuits raw data

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

circuits_schema = StructType(fields=[StructField("circuitId",IntegerType(), False),
                                     StructField('circuitRef', StringType(), True),
                                     StructField('name', StringType(), True),
                                     StructField('location', StringType(), True), 
                                     StructField('country', StringType(), True), 
                                     StructField('lat', DoubleType(), True), 
                                     StructField('lng', DoubleType(), True), 
                                     StructField('alt', IntegerType(), True), 
                                     StructField('url', StringType(), True)])

circuits_df = (spark.read
               .option("header", True)
               .schema(circuits_schema)
               .csv(f'{raw_folder_path}/circuits.csv', header = True))

# COMMAND ----------

(
    circuits_df.write
            .format('delta')
            .option('path',f'{raw_abfss}/circuits')
            .saveAsTable("f1_raw.circuits")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE f1_raw.circuits

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED f1_raw.circuits

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Ingest Races raw data

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.races;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.races(
# MAGIC   raceId INT,
# MAGIC   year INT,
# MAGIC   round INT,
# MAGIC   circuitId INT,
# MAGIC   name STRING,
# MAGIC   date DATE,
# MAGIC   time STRING,
# MAGIC   url STRING)
# MAGIC USING csv
# MAGIC OPTIONS (path "abfss://raw@f1dbhello.dfs.core.windows.net/races.csv", header true)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED f1_raw.constructors;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest from JSON

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Ingest Constructors

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.constructors;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.constructors(
# MAGIC constructorId INT,
# MAGIC constructorRef STRING,
# MAGIC name STRING,
# MAGIC nationality STRING,
# MAGIC url STRING)
# MAGIC USING json
# MAGIC OPTIONS(path "abfss://raw@f1dbhello.dfs.core.windows.net/constructors.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Ingest Drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.drivers;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.drivers(
# MAGIC driverId INT,
# MAGIC driverRef STRING,
# MAGIC number INT,
# MAGIC code STRING,
# MAGIC name STRUCT<forename: STRING, surname: STRING>,
# MAGIC dob DATE,
# MAGIC nationality STRING,
# MAGIC url STRING)
# MAGIC USING json
# MAGIC OPTIONS (path "abfss://raw@f1dbhello.dfs.core.windows.net/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Ingest from Multiline JSON

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.results;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.results(
# MAGIC resultId INT,
# MAGIC raceId INT,
# MAGIC driverId INT,
# MAGIC constructorId INT,
# MAGIC number INT,grid INT,
# MAGIC position INT,
# MAGIC positionText STRING,
# MAGIC positionOrder INT,
# MAGIC points INT,
# MAGIC laps INT,
# MAGIC time STRING,
# MAGIC milliseconds INT,
# MAGIC fastestLap INT,
# MAGIC rank INT,
# MAGIC fastestLapTime STRING,
# MAGIC fastestLapSpeed FLOAT,
# MAGIC statusId STRING)
# MAGIC USING json
# MAGIC OPTIONS(path "abfss://raw@f1dbhello.dfs.core.windows.net/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Ingest Results

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 - Ingest Pitstops

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.pit_stops;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
# MAGIC driverId INT,
# MAGIC duration STRING,
# MAGIC lap INT,
# MAGIC milliseconds INT,
# MAGIC raceId INT,
# MAGIC stop INT,
# MAGIC time STRING)
# MAGIC USING json
# MAGIC OPTIONS(path "abfss://raw@f1dbhello.dfs.core.windows.net/pit_stops.json", multiLine true)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Ingest from multiple files

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7 - Ingest Lap Times

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.lap_times;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
# MAGIC raceId INT,
# MAGIC driverId INT,
# MAGIC lap INT,
# MAGIC position INT,
# MAGIC time STRING,
# MAGIC milliseconds INT
# MAGIC )
# MAGIC USING csv
# MAGIC OPTIONS (path "abfss://raw@f1dbhello.dfs.core.windows.net/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8 - Ingest qualifying

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.qualifying;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
# MAGIC constructorId INT,
# MAGIC driverId INT,
# MAGIC number INT,
# MAGIC position INT,
# MAGIC q1 STRING,
# MAGIC q2 STRING,
# MAGIC q3 STRING,
# MAGIC qualifyId INT,
# MAGIC raceId INT)
# MAGIC USING json
# MAGIC OPTIONS (path "abfss://raw@f1dbhello.dfs.core.windows.net/qualifying", multiLine true)
