# Databricks notebook source
# MAGIC %md
# MAGIC ### Streaming Multi-Hop Archtecture
# MAGIC * Multi-hope pipeline – processing data in successive stages called Bronze, Silver and Gold. A single or all stages can be used depending on business use case
# MAGIC * Bronze – these tables contain raw data ingested from various sources i.e., JSON files, RDBMS data,  IoT data, etc.
# MAGIC * Silver – it provides more refined view of the data using any transformation logic, for example, adding new columns, joining with static table etc. 
# MAGIC * Gold – operation like data aggregation is accomplished at this level suitable for reporting and dashboarding, for example, daily active website users, weekly sales per store, or gross revenue per quarter by department etc. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notebook Setup
# MAGIC * It will run the init script to generate userhome path 
# MAGIC * Subsequently, we create data storage and streaming checkpoint location 
# MAGIC * Reset mode will cleanup existing and generate userhome path, database (HIVE metastore) name etc.
# MAGIC * Clean up mode will remove all data in userhome dbfs location and the database 

# COMMAND ----------

# MAGIC %run ./fraud-stream-setup-production

# COMMAND ----------

# MAGIC %run ./credit-fraud-udfs

# COMMAND ----------

# MAGIC %fs rm wasbs://output@vmsampstorage.blob.core.windows.net/trigger.txt

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Machine Learning Model Pipeline  
# MAGIC * Load the saved model in DBFS using ML Pipelines 

# COMMAND ----------

from pyspark.ml import PipelineModel

pipelinePath = "dbfs:/FileStore/tables/cr_fraud_pipeline_model"
savedPipelineModel = PipelineModel.load(pipelinePath)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming Data Simulator
# MAGIC * Previous genered raw data (create df-to-json script) was loaded to a Filestore location 
# MAGIC * JSON files containing raw data is copied to the streaming path 
# MAGIC * This data will be subsequently ingested by the readStream 

# COMMAND ----------

# MAGIC %fs ls wasbs://landing@vmsampstorage.blob.core.windows.net/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest raw data 
# MAGIC * Bronze stage will read JSON as a text file and the data will be kept in the raw form
# MAGIC * Spark DataFrame API is used to set up a streaming read, once configured, it will be registered in a temp view to leverage Spark SQL for transformations 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Data with Auto Loader
# MAGIC * Databricks Auto Loader is used for streaming raw data from cloud object storage
# MAGIC * Configuring Auto Loader requires using the *cloudFiles* format
# MAGIC * It is done by replacing file format with *cloudFiles*, and add the file type as a string for the option *cloudFiles.format*

# COMMAND ----------

(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "text")
  .schema("data STRING")
  .option("maxFilesPerTrigger", 1)  
  .load(streamingPath)
  .createOrReplaceTempView("transactions_raw_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze stage transformation
# MAGIC * Encoding the receipt time and the name of the dataset will provide flexibility to use a same bronze table for multiple purpose and pipelines
# MAGIC * This multiplex table design replicates the semi-structured nature of data stored in most data lakes 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW transactions_bronze_temp AS (
# MAGIC   SELECT current_timestamp() receipt_time, "transactions" dataset, *
# MAGIC   FROM transactions_raw_temp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM transactions_bronze_temp

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Streaming output using Delta Lake
# MAGIC * Write a streaming output to a Delta Lake table
# MAGIC * outputMode is append meaning add only new records to output sink
# MAGIC * Location of a **checkpoint** directory is specified 
# MAGIC * The purpose **checkpoint** is to stores the current state of the streaming, if stops and resumes, will continue from where it left off
# MAGIC * Without checkpoint directory streaming job will resume from scratch
# MAGIC * In this demo, streaming job have its own checkpoint directory (it can't be shared)

# COMMAND ----------

(spark.table("transactions_bronze_temp")
  .writeStream
  .format("delta")
  .option("checkpointLocation", bronzeCheckpoint)
  .outputMode("append")
  .start(bronzePath))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver stage transformation 
# MAGIC * In the first silver stage transformation, we will use transactions dataset and parse the JSON payload
# MAGIC * JSON payload is parsed enforcing the schema 

# COMMAND ----------

time.sleep(30)

# COMMAND ----------

(spark.readStream
  .format("delta")
  .load(bronzePath)
  .createOrReplaceTempView("bronze_unparsed_temp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW transactions_parsed_temp AS
# MAGIC   SELECT 
# MAGIC     json.step step, 
# MAGIC     json.type type, 
# MAGIC     json.amount amount, 
# MAGIC     json.nameOrig nameOrig, 
# MAGIC     json.oldbalanceOrg oldbalanceOrg, 
# MAGIC     json.newbalanceOrig newbalanceOrig, 
# MAGIC     json.nameDest nameDest, 
# MAGIC     json.oldbalanceDest oldbalanceDest,
# MAGIC     json.newbalanceDest newbalanceDest, 
# MAGIC     json.isFraud isFraud, 
# MAGIC     json.isFlaggedFraud isFlaggedFraud
# MAGIC   FROM (
# MAGIC     SELECT from_json(data, "step INTEGER, 
# MAGIC                             type STRING, 
# MAGIC                             amount DOUBLE, 
# MAGIC                             nameOrig STRING,
# MAGIC                             oldbalanceOrg DOUBLE, 
# MAGIC                             newbalanceOrig DOUBLE, 
# MAGIC                             nameDest STRING, 
# MAGIC                             oldbalanceDest DOUBLE,
# MAGIC                             newbalanceDest DOUBLE, 
# MAGIC                             isFraud INTEGER, 
# MAGIC                             isFlaggedFraud INTEGER") json
# MAGIC     FROM bronze_unparsed_temp
# MAGIC     WHERE dataset = "transactions")

# COMMAND ----------

(spark.table("transactions_parsed_temp")
  .writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", transactionsParsedCheckpoint)
  .start(transactionsParsedPath))

# COMMAND ----------

# MAGIC %md
# MAGIC * In second silver stage transformation, we will modifiy the data to match with the training dataset
# MAGIC * It involves adding transaction and hour column
# MAGIC * Drop columns not required for the model to work i.e., isFlag
# MAGIC * In this case, we will keep nameOrig column (customer who started the transaction) to trace back and notify if predicted fraud 

# COMMAND ----------

time.sleep(30)

# COMMAND ----------

(spark.readStream
  .format("delta")
  .load(transactionsParsedPath)
  .createOrReplaceTempView("silver_transactions_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load static member data containing
# MAGIC * nameOrig (to identify transaction owner)
# MAGIC * username (assume 3 users for the demo)
# MAGIC * webhook url to send slack notification

# COMMAND ----------

(spark
  .read
  .format("csv")
  .schema("nameOrig STRING, userid STRING, webhook_url STRING")
  .option("header", True)
  .load(f"{source_dir}/user-outreach.csv")
  .createOrReplaceTempView("user_outreach"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM user_outreach LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW transactions_refined AS
# MAGIC   SELECT * FROM
# MAGIC   (
# MAGIC   (SELECT step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig, oldbalanceDest, newbalanceDest,
# MAGIC   CONCAT(SUBSTR(nameOrig, 1, 1), SUBSTR(nameDest, 1, 1)) AS transaction, MOD(step, 24) AS hour
# MAGIC   FROM silver_transactions_temp)
# MAGIC   LEFT JOIN
# MAGIC   (SELECT * FROM user_outreach)
# MAGIC   USING (nameOrig)
# MAGIC   )

# COMMAND ----------

(spark.table("transactions_refined")
  .writeStream
  .format("delta")
  .option("checkpointLocation", transactionsEnrichedCheckpoint)
  .outputMode("append")
  .start(transactionsEnrichedPath))

# COMMAND ----------

# MAGIC %md
# MAGIC * Read stream and apply the classification model 

# COMMAND ----------

time.sleep(30)

# COMMAND ----------

streamingData = (spark
                 .readStream
                 .option("maxFilesPerTrigger", 1)
                 .format("delta")
                 .load(transactionsEnrichedPath))

# COMMAND ----------

savedPipelineModel.transform(streamingData).createOrReplaceTempView("streaming_prediction")

# COMMAND ----------

# streamPred = savedPipelineModel.transform(streamingData)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display model output 
# MAGIC * Last four columns of the table provide model outputs in terms of:
# MAGIC   * features used for prediction
# MAGIC   * Raw prediction (logits in this case)
# MAGIC   * Probability of obtaining each class
# MAGIC   * And finally, the prediction, in terms of 0 and 1, here 1 being the predicted fraud transaction

# COMMAND ----------

# display(streamPred)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Slack notification 
# MAGIC * Apply the registered slack notification function `slack_notification`
# MAGIC   * Function has three arguments i.e. prediction, userid and webhook_url
# MAGIC   * It evaluates each prediction output and send notification to the account owner
# MAGIC   * In purpose of the demo, only three user account was used user01, user02 and user03
# MAGIC   * In real production scenerio, there will be far more users and far less fraud transaction (output '1')

# COMMAND ----------

# MAGIC %sql
# MAGIC SElECT *,
# MAGIC slack_notification(prediction, userid, webhook_url) as notify_user
# MAGIC FROM streaming_prediction

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stop all streams

# COMMAND ----------

stop_stream(10, 10, 200)
