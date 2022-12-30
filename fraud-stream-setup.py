# Databricks notebook source
# MAGIC %run ./fraud-stream-init $course="sslh" $mode="reset"

# COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)

# COMMAND ----------

streamingPath          = userhome + "/source"
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


