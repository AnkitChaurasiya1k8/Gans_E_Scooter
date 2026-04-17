# Databricks notebook source
# MAGIC %md
# MAGIC out_of_100
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum, round

bronze_base = "abfss://bronze@escooterstorage123.dfs.core.windows.net"

# READ FINAL SILVER
df = spark.read.csv(
    f"{bronze_base}/final_expected_demand.csv",
    header=True,
    inferSchema=True
)

# TOTAL DEMAND
total_demand = df.agg(spark_sum("expected_demand")).collect()[0][0]

# CALCULATE CONTRIBUTION
gold_df = df.withColumn(
    "contribution",
    round((col("expected_demand") / total_demand) * 100, 2)
).select("city", "contribution")

display(gold_df)

# COMMAND ----------

temp_path = f"{bronze_base}/temp_gold_contribution"
final_path = f"{bronze_base}/gold_contribution.csv"

gold_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(temp_path)

files = dbutils.fs.ls(temp_path)
csv_file = [f.path for f in files if f.path.endswith(".csv")][0]

dbutils.fs.mv(csv_file, final_path)
dbutils.fs.rm(temp_path, True)

display(dbutils.fs.ls(bronze_base))

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

