# Databricks notebook source
time_data = []

for hour in range(24):
    
    if 0 <= hour <= 5:
        value = 0
    elif hour == 6:
        value = 2
    elif hour == 7:
        value = 4
    elif hour == 8:
        value = 6
    elif hour == 9:
        value = 10 # office time
    elif 10 <= hour <= 11:
        value = 6
    elif 12 <= hour <= 14:
        value = 10 # food delievery
    elif 14 <= hour <= 16:
        value = 4
    elif hour == 17:
        value = 6
    elif hour == 18:# office time
        value = 8
    elif 19 <= hour <= 21: # dinner time
        value = 10
    else:
        value = 2
    
    time_data.append((hour, value))

time_df = spark.createDataFrame(time_data, ["hour", "time_value"])

display(time_df)

# COMMAND ----------

temp_path = "abfss://bronze@escooterstorage123.dfs.core.windows.net/temp_time"
final_path = "abfss://bronze@escooterstorage123.dfs.core.windows.net/time_value.csv"

time_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(temp_path)

files = dbutils.fs.ls(temp_path)
csv_file = [f.path for f in files if f.path.endswith(".csv")][0]

dbutils.fs.mv(csv_file, final_path)
dbutils.fs.rm(temp_path, True)