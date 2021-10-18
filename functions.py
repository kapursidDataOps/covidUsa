# Databricks notebook source
import pyspark.sql.functions 
from pyspark.sql.functions import col
from pyspark.sql.functions import sum
from pyspark.sql.types import *

# COMMAND ----------

# Azure storage access info
blob_account_name = "pandemicdatalake"
blob_container_name = "public"
blob_relative_path = "curated/covid-19/covid_tracking/latest/covid_tracking.parquet"
blob_sas_token = r""

# Allow SPARK to read from Blob remotely
wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)
spark.conf.set(
  'fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name),
  blob_sas_token)
print('Remote blob path: ' + wasbs_path)

# COMMAND ----------

spark.conf.set("fs.azure.account.key.adlsgen290498.dfs.core.windows.net",config.AccessKey)
parquetUsState = "abfss://us5tates@adlsgen290498.dfs.core.windows.net/Us_States.xlsx.parquet"

# COMMAND ----------

JanFebCovid = 'abfss://analysis@adlsgen290498.dfs.core.windows.net/raw/covid2021/JanFebCovid'
