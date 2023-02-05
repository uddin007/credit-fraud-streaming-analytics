# Databricks notebook source
# MAGIC %md
# MAGIC Functions to automatically shutdown (gracefully) all streams 

# COMMAND ----------

from timeit import default_timer as timer
import time

# COMMAND ----------

def latest_time(sleeptime):
  x = dbutils.fs.ls("wasbs://landing@vmsampstorage.blob.core.windows.net/")
  
  y = 0
  for i in range(len(dbutils.fs.ls("wasbs://landing@vmsampstorage.blob.core.windows.net/"))):
    x = dbutils.fs.ls("wasbs://landing@vmsampstorage.blob.core.windows.net/")
    time.sleep(sleeptime)
    if x[i][3] > y:
      z = x[i][3]
      y = z
    else:
      z = y
      
  return(z)

# COMMAND ----------

def stop_stream(check_interval, sleeptime, wait_time):
  prev_timestamp = 0
  value = True
  start = timer()

  while value:
    last_timestamp = latest_time(sleeptime)
    time.sleep(check_interval)
    if last_timestamp == prev_timestamp:
      end = timer() 
    else:
      start = timer()

    diff = timer() - start
    prev_timestamp = last_timestamp

    print(prev_timestamp)
    print(last_timestamp)
    print(diff)
    print('-----')

    if diff > wait_time:
      for s in spark.streams.active:
        s.stop()
      dbutils.fs.cp("wasbs://input@vmsampstorage.blob.core.windows.net/trigger.txt", "wasbs://output@vmsampstorage.blob.core.windows.net/")
      break

# COMMAND ----------

# MAGIC %md
# MAGIC Functions to send slack notification based on model output 

# COMMAND ----------

import datetime
import urllib3
import json
import traceback

def slacknotif(output, user, webhook_url):
  if output == 1:
    try:
      message = f"{user}, a fraud transaction is posted in your account at {datetime.datetime.now()}"
      slack_message = {'text': message}
      http = urllib3.PoolManager()
      response = http.request('POST',
                                webhook_url,
                                body = json.dumps(slack_message),
                                headers = {'Content-Type': 'application/json'},
                                retries = False)
    except:
      traceback.print_exc()
      
    return ('Notification sent')
  
  else:
    return('Valid transaction')

spark.udf.register("slack_notification", slacknotif)

# COMMAND ----------


