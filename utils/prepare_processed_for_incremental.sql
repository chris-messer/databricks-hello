-- Databricks notebook source
DROP DATABASE f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
MANAGED LOCATION 'abfss://presentation@f1dbhello.dfs.core.windows.net';

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
MANAGED LOCATION 'abfss://processed@f1dbhello.dfs.core.windows.net';
