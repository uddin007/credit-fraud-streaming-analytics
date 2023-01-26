# Databricks notebook source
# MAGIC %run ./fraud-stream-init $course="sslh" $mode="reset"

# COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)

# COMMAND ----------

storage_account_name = dbutils.secrets.get(scope="socialAnalyticScope", key="azurestoragename")
storage_account_access_key = dbutils.secrets.get(scope="socialAnalyticScope", key="azurestoragekey")

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

blob_container = 'landing'

streamingPath = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/"

# COMMAND ----------

# streamingPath          = userhome + "/source"
bronzePath             = userhome + "/bronze"
transactionsParsedPath   = userhome + "/silver/transactions_parsed"
transactionsEnrichedPath = userhome + "/silver/transactions_enriched"

checkpointPath                 = userhome + "/checkpoints"
bronzeCheckpoint               = userhome + "/checkpoints/bronze"
transactionsParsedCheckpoint   = userhome + "/checkpoints/transactions_parsed"
transactionsEnrichedCheckpoint = userhome + "/checkpoints/transactions_enriched"

# COMMAND ----------

print(f"""
streamingPath: {streamingPath}
bronzePath: {bronzePath}
transactionsParsedPath: {transactionsParsedPath}
transactionsEnrichedPath: {transactionsEnrichedPath}

checkpointPath: {checkpointPath}
bronzeCheckpoint: {bronzeCheckpoint}
transactionsParsedCheckpoint: {transactionsParsedCheckpoint}
transactionsEnrichedCheckpoint: {transactionsEnrichedCheckpoint}
""")

# COMMAND ----------


