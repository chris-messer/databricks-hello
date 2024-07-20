-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
MANAGED LOCATION 'abfss://processed@f1dbhello.dfs.core.windows.net';

-- COMMAND ----------

DESC  DATABASE EXTENDED f1_processed;

-- COMMAND ----------


