# credit-fraud-streaming-analytics
The project is focused on developing real-time fraud detection using Spark Structured Streaming and Streaming Analytics.

The general development data pipeline is shown below. 

![streaming-analytics-v01](https://user-images.githubusercontent.com/37245809/215386111-2c725bf9-a5f9-40c0-a439-96c0d4ae56ef.png)

A brief summary of development architecture and components are as follows.

### Streaming Multi-Hop Archtecture
* Multi-hope pipeline – processing data in successive stages called Bronze, Silver and Gold. A single or all stages can be used depending on business use case
* Bronze – these tables contain raw data ingested from various sources i.e., JSON files, RDBMS data,  IoT data, etc.
* Silver – it provides more refined view of the data using any transformation logic, for example, adding new columns, joining with static table etc. 
* Gold – operation like data aggregation is accomplished at this level suitable for reporting and dashboarding, for example, daily active website users, weekly sales per store, or gross revenue per quarter by department etc. 

### Notebook Setup
* It will run the init script to generate userhome path 
* Subsequently, we create data storage and streaming checkpoint location 
* Reset mode will cleanup existing and generate userhome path, database (HIVE metastore) name etc.
* Clean up mode will remove all data in userhome dbfs location and the database 

### Load Machine Learning Model Pipeline  
* Load the saved model in DBFS using ML Pipelines 

### Streaming Data Simulator
* Previous genered raw data (create df-to-json script) was loaded to a Filestore location 
* JSON files containing raw data is copied to the streaming path 
* This data will be subsequently ingested by the readStream 

### Ingest raw data 
* Bronze stage will read JSON as a text file and the data will be kept in the raw form
* Spark DataFrame API is used to set up a streaming read, once configured, it will be registered in a temp view to leverage Spark SQL for transformations 

### Ingest Data with Auto Loader
* Databricks Auto Loader is used for streaming raw data from cloud object storage
* Configuring Auto Loader requires using the *cloudFiles* format
* It is done by replacing file format with *cloudFiles*, and add the file type as a string for the option *cloudFiles.format*

### Bronze stage transformation
* Encoding the receipt time and the name of the dataset will provide flexibility to use a same bronze table for multiple purpose and pipelines
* This multiplex table design replicates the semi-structured nature of data stored in most data lakes 

### Write Streaming output using Delta Lake
* Write a streaming output to a Delta Lake table
* outputMode is append meaning add only new records to output sink
* Location of a **checkpoint** directory is specified 
* The purpose **checkpoint** is to stores the current state of the streaming, if stops and resumes, will continue from where it left off
* Without checkpoint directory streaming job will resume from scratch
* In this demo, streaming job have its own checkpoint directory

### Silver stage transformation 
* In the first silver stage transformation, we will use transactions dataset and parse the JSON payload
* JSON payload is parsed enforcing the schema 

### More transformation in silver stage 
* In second silver stage transformation, we will modifiy the data to match with the training dataset
* It involves adding transaction and hour column
* Drop columns not required for the model to work i.e., isFlag

### Apply machine learning model
* Read the stream 
* The logistic regression classification model developed and saved previous is applied to the streaming data 
* In this case, we kept nameOrig column (customer who started the transaction) to trace back and notify if predicted fraud 

### Display model output 
* Last four columns of the table provide model outputs in terms of:
  * features used for prediction
  * Raw prediction (logits in this case)
  * Probability of obtaining each class
  * And finally, the prediction, in terms of 0 and 1, here 1 being the predicted fraud transaction

The general production data pipeline is shown below.

![streaming-analytics-production-v01](https://user-images.githubusercontent.com/37245809/215386419-1e606355-832e-4101-9b1b-e04000d26256.png)

A brief summary of production architecture and components are as follows. Azure Data Factory (ADF) is used as the orchestration framework. 

### Azure BlobEventsTrigger
* Create pipeline trigger based on new file arrival at source container 
* The objective is to trigger the pipeline as new streaming data arrives 
* Type of trigger is BlobEventsTrigger where 'blob path ends with' .json

### ADF pipeline 
* Pipeline starts with 'Get Metadata' to check if specific file exists, this is to ensure Databricks Job is not created while one is already running. 
* The second part of the pipeline is an IF Condition. Based on previous event, if a job is not already running, it will move to the next stage i.e. to trigger a Databricks job
* Finally, a Databricks Job is triggered with the specified Job Cluster configured using the Linked Service. 

### Databricks Job termination
* This is to ensure a Job is not continuously running if there is no new data at the source container.
* Python UDFs are developed and applied to monitor new file arrivals at source location.
* If files are not arriving for predefined period, UDFs are used to Gracefully Shutdown all active streams.
* This will subsequently terminate the job cluster 

### Data storage and notification
* Under development 
