# Databricks notebook source
raw_folder_path = '/mnt/f1dbhello/raw'
processed_folder_path = '/mnt/f1dbhello/processed'
presentation_folder_path = '/mnt/f1dbhello/presentation'

# COMMAND ----------

raw_abfss = "abfss://raw@f1dbhello.dfs.core.windows.net"
processed_folder_path = "abfss://processed@f1dbhello.dfs.core.windows.net"
presentation_folder_path = "abfss://presentation@f1dbhello.dfs.core.windows.net"
