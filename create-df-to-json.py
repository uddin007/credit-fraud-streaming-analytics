# Databricks notebook source
from pyspark.sql.functions import when, col, lit, mean, isnan, count, concat

# COMMAND ----------

storage_account_name = "vmsampstorage"
storage_account_access_key = "dTTYh95gBmOUTB5bJwqp8xNFec5z1Y1ARCj5ToE6iG3mGQKBp25SLIBkuCm1XBG62uF4Q8a9BOA0+AStrVqaZg=="

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

blob_container = 'input'

filePath = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/PS_20174392719_1491204439457_log.csv"

inputDF = spark.read.format("csv").load(filePath, inferSchema = True, header = True)

# COMMAND ----------

inputDF.display()

# COMMAND ----------

inputDF.groupBy('isFraud').count().show()

# COMMAND ----------

inputBalDF = inputDF.sampleBy("isFraud", fractions={1: 0.4, 0: 0.001})
inputBalDF.groupBy('isFraud').count().show()

# COMMAND ----------

import numpy as np
import pandas as pd

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Convert the Spark DataFrame back to a pandas DataFrame using Arrow
result_pdf = inputBalDF.select("*").toPandas()
result_pdf.head()

# COMMAND ----------

json_out_03 = result_pdf.to_json(orient='records', lines=True)
print(json_out_03)

# COMMAND ----------

dbutils.fs.put("/FileStore/tables/cr-out-03.json", json_out_03)

# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/tables/cr-out-03.json

# COMMAND ----------


