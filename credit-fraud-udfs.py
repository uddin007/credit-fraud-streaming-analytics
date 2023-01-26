# Databricks notebook source
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
