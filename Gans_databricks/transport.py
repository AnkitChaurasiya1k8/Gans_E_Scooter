# Databricks notebook source
# MAGIC %pip install lxml

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.functions import col

bronze_base = "abfss://bronze@escooterstorage123.dfs.core.windows.net"

# READ movement.csv
movement_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{bronze_base}/movements.csv")

# COMPUTE POPULATION MOVEMENT
transport_df = movement_df \
    .withColumn("flight_pop", col("flights_per_day") * 200) \
    .withColumn("rail_pop", col("trains_per_day") * 1500)

display(transport_df)

# COMMAND ----------

temp_path = f"{bronze_base}/temp_transport"
final_path = f"{bronze_base}/transport.csv"

transport_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(temp_path)

files = dbutils.fs.ls(temp_path)
csv_file = [f.path for f in files if f.path.endswith(".csv")][0]

dbutils.fs.mv(csv_file, final_path)
dbutils.fs.rm(temp_path, True)

display(dbutils.fs.ls(bronze_base))

# COMMAND ----------

