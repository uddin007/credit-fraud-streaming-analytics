# Databricks notebook source
# MAGIC %md
# MAGIC ## Load data

# COMMAND ----------

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

# MAGIC %md
# MAGIC ### Stratified sampling 

# COMMAND ----------

inputBalDF = inputDF.sampleBy("isFraud", fractions={1: 1, 0: 0.004}, seed=0)
inputBalDF.groupBy('isFraud').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add new features 
# MAGIC * Remove records where label is missing
# MAGIC * Add feature transaction type from "C" (customer) and "M" (merchant) using nameOrig and nameDest
# MAGIC * Feature values are "CC" (Customer to Customer), "CM" (Customer to Merchant), "MC" (Merchant to Customer), "MM" (Merchant to Merchant)

# COMMAND ----------

inputBalDF = inputBalDF.filter(inputBalDF.isFraud.isNotNull())
inputAddDF = inputBalDF.withColumn('transaction', concat(inputBalDF.nameOrig.substr(1, 1), inputBalDF.nameDest.substr(1, 1)))
inputAddDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Replace & drop values 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create label column

# COMMAND ----------

labelDF = inputAddDF.select(when(col("isFraud") == "1", 1).otherwise(0).alias("label"), "*").drop("isFraud","isFlaggedFraud","nameOrig","nameDest")
labelDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Check for missing values
# MAGIC *Note: if there's no missing values, no reason to perform imputation*

# COMMAND ----------

missingDF = labelDF.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in labelDF.columns])
missingDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Impute missing values 
# MAGIC * Drop row if label is missing (done above)
# MAGIC * Replace missing values with mean for numerical columns
# MAGIC * Replace missing values with mode with categorical columns 

# COMMAND ----------

cleanDF = labelDF

# categoricalCols = [ field for (field, dataType) in labelDF.dtypes if ((dataType == "string") & (field != 'label')) ]
# numericCols = [ field for (field, dataType) in labelDF.dtypes if ((dataType != "string") & (field != 'label')) ]

# for c in categoricalCols:
#   colMode = labelDF.groupby(c).count().orderBy("count", ascending=False).first()[0]
#   cleanDF = labelDF.withColumn(c, when(col(c).isNull(), colMode).otherwise(labelDF[c]))  
  
# for c in numericCols:
#   colMean = labelDF.agg({c: 'avg'}).first()[0]
#   cleanDF = labelDF.withColumn(c, when(col(c).isNull(), colMean).otherwise(labelDF[c]))
  
cleanDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add hour column
# MAGIC * We observe that from hour 0 to hour 9 valid transactions very seldom occur. On the other hand, fraudulent transactions still occur at similar rates to any hour of the day outside of hours 0 to 9
# MAGIC * In response to this, We1m will create another feature HourOfDay, which is the step column with each number taken to modulo 24

# COMMAND ----------

cleanDF = cleanDF.withColumn("hour", col("step") % 24)
# cleanDF.filter(cleanDF.hour == 0).display()
# cleanDF.groupBy('hour').count().display()
cleanDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add weight column 
# MAGIC * Calculate weight
# MAGIC * Add weight 

# COMMAND ----------

# label_1 = cleanDF.filter(cleanDF.label == 1).count()
# label_0 = cleanDF.filter(cleanDF.label == 0).count()
# ratio = label_1/label_0

# label_1_wt  = 1- ratio
# label_0_wt = ratio

# print(label_1)
# print(label_0)
# print(label_1_wt)
# print(label_0_wt)

# COMMAND ----------

# weightedDF = cleanDF.withColumn("weight", when(col("label") == '1', 0.6).otherwise(0.4))
# weightedDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train/Test Split
# MAGIC * Use the same 80/20 split with a seed 

# COMMAND ----------

(trainDF, testDF) = cleanDF.randomSplit([.8, .2], seed=42)
print(trainDF.cache().count())

# COMMAND ----------

trainDF.groupBy('label').count().show()

# COMMAND ----------

from pyspark.ml.feature import OneHotEncoder, StringIndexer

categoricalCols = [field for (field, dataType) in trainDF.dtypes if dataType == "string"]
indexOutputCols = [x + "Index" for x in categoricalCols]
oheOutputCols = [x + "OHE" for x in categoricalCols]

stringIndexer = StringIndexer(inputCols=categoricalCols, outputCols=indexOutputCols, handleInvalid="skip")
oheEncoder = OneHotEncoder(inputCols=indexOutputCols, outputCols=oheOutputCols)

print(categoricalCols)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vector Assembler
# MAGIC * Now we can combine our OHE categorical features with our numeric features.

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

numericCols = [field for (field, dataType) in trainDF.dtypes if ((dataType != "string") & (field != "label"))]
assemblerInputs = oheOutputCols + numericCols
vecAssembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")

print(numericCols)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluate model
# MAGIC * For right now, let's use accuracy as our metric. This is available from MulticlassClassificationEvaluator 

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

mcEvaluator = MulticlassClassificationEvaluator(metricName="accuracy")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Logistic Regression
# MAGIC * Build a logistic regression model 

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import RFormula
from pyspark.ml.classification import LogisticRegression

rFormula = RFormula(formula="label ~ .", featuresCol="features", labelCol="label", handleInvalid="skip") 
lr = LogisticRegression(labelCol="label", featuresCol="features")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Hyperparameter Tuning
# MAGIC * Changing the hyperparameters of the logistic regression model using the cross-validator 

# COMMAND ----------

from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.tuning import CrossValidator

paramGrid = (ParamGridBuilder()
            .addGrid(lr.regParam, [0.1, 0.2, 1.0])
            .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
            .build())

cv = CrossValidator(estimator=lr, evaluator=mcEvaluator, estimatorParamMaps=paramGrid,
                    numFolds=3, parallelism=4, seed=42)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create model pipeline
# MAGIC * Create pipeline with stages properly attached

# COMMAND ----------

pipeline = Pipeline(stages=[rFormula, cv])

pipelineModel = pipeline.fit(trainDF)

predDF = pipelineModel.transform(testDF)

# COMMAND ----------

predDF.display()

# COMMAND ----------

from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

mcEvaluator = MulticlassClassificationEvaluator(metricName="accuracy")
print(f"The accuracy is {100*mcEvaluator.evaluate(predDF):.2f}%")

bcEvaluator = BinaryClassificationEvaluator(metricName="areaUnderROC")
print(f"The area under the ROC curve: {bcEvaluator.evaluate(predDF):.2f}")

bcEvaluator.setMetricName("areaUnderPR")
print(f"The area under the PR curve: {bcEvaluator.evaluate(predDF):.2f}")

# COMMAND ----------

import sklearn 

y_true = predDF.select(['label']).collect()
y_pred = predDF.select(['prediction']).collect()

from sklearn.metrics import classification_report, confusion_matrix
print(classification_report(y_true, y_pred))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving Models
# MAGIC 
# MAGIC We can save our models to persistent storage (e.g. DBFS) in case our cluster goes down so we don't have to recompute our results.

# COMMAND ----------

pipelinePath = "dbfs:/FileStore/tables/cr_fraud_pipeline_model"
pipelineModel.write().overwrite().save(pipelinePath)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading models
# MAGIC 
# MAGIC When you load in models, you need to know the type of model you are loading back in (was it a linear regression or logistic regression model?).
# MAGIC 
# MAGIC For this reason, we recommend you always put your transformers/estimators into a Pipeline, so you can always load the generic PipelineModel back in.

# COMMAND ----------

from pyspark.ml import PipelineModel

savedPipelineModel = PipelineModel.load(pipelinePath)

# COMMAND ----------

print(savedPipelineModel)

# COMMAND ----------


