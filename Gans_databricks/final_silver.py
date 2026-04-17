# Databricks notebook source
from pyspark.sql.functions import col, round

bronze_base = "abfss://bronze@escooterstorage123.dfs.core.windows.net"

# READ FINAL SCORES TABLE
df = spark.read.csv(
    f"{bronze_base}/silver_all_scores.csv",
    header=True,
    inferSchema=True
)

# READ TIME TABLE (unchanged logic)
time_df = spark.read.csv(
    f"{bronze_base}/silver_time_score.csv",
    header=True,
    inferSchema=True
)

# GET CURRENT HOUR
current_hour_df = spark.sql("SELECT hour(current_timestamp()) AS hour")

# GET TIME SCORE FOR CURRENT HOUR
time_score = current_hour_df.join(time_df, "hour") \
    .select("time_score") \
    .collect()[0]["time_score"]

# HANDLE NULLS (safe step)
df = df.fillna(0)

# FINAL EXPECTED DEMAND MODEL
df = df.withColumn(
    "expected_demand",
    round(
        col("city_score") * 0.20 +
        col("transport_score") * 0.20 +
        col("poi_score") * 0.15 +
        col("revenue_score") * 0.15 +
        col("weather_score") * 0.10 +
        col("safety_score") * 0.10 +
        (time_score * 0.10),
        2
    )
)

# FINAL OUTPUT
final_df = df.select("city", "expected_demand")

display(final_df)

# COMMAND ----------

from pyspark.sql.functions import col

# Order by demand (descending)
final_df = final_df.orderBy(col("expected_demand").desc())

# Display sorted result
display(final_df)

# COMMAND ----------

# Save
temp_path = f"{bronze_base}/temp_final_demand"
final_path = f"{bronze_base}/final_expected_demand.csv"

final_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(temp_path)

files = dbutils.fs.ls(temp_path)
csv_file = [f.path for f in files if f.path.endswith(".csv")][0]

dbutils.fs.mv(csv_file, final_path)
dbutils.fs.rm(temp_path, True)

display(dbutils.fs.ls(bronze_base))