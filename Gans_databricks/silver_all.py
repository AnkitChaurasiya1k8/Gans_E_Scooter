# Databricks notebook source
# MAGIC %md
# MAGIC silver revenue
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum, round

bronze_base = "abfss://bronze@escooterstorage123.dfs.core.windows.net"

# READ DATA
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{bronze_base}/third_parties.csv")

# CITY TOTAL REVENUE
df = df.withColumn(
    "city_revenue",
    col("revenue_delivery_apps") + col("revenue_ride_sharing")
)

# TOTAL REVENUE (ALL CITIES)
total_revenue = df.agg(_sum("city_revenue").alias("total")).collect()[0]["total"]

# REVENUE CONTRIBUTION %
df = df.withColumn(
    "revenue_score",
    round((col("city_revenue") / total_revenue) * 100, 2)
)

# FINAL OUTPUT
final_df = df.select(
    "city",
    "revenue_delivery_apps",
    "revenue_ride_sharing",
    "city_revenue",
    "revenue_score"
)

display(final_df)

# COMMAND ----------

from pyspark.sql.functions import col

# KEEP ONLY REQUIRED COLUMNS
final_df = df.select("city", "revenue_score")

# PATHS
temp_path = f"{bronze_base}/temp_silver_revenue"
final_path = f"{bronze_base}/silver_revenue_score.csv"

# WRITE TO TEMP FOLDER
final_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(temp_path)

# FIND GENERATED FILE
files = dbutils.fs.ls(temp_path)
csv_file = [f.path for f in files if f.path.endswith(".csv")][0]

# MOVE TO FINAL LOCATION
dbutils.fs.mv(csv_file, final_path)

# DELETE TEMP FOLDER
dbutils.fs.rm(temp_path, True)

# VERIFY
display(dbutils.fs.ls(bronze_base))

# COMMAND ----------

# MAGIC %md
# MAGIC silver_cities_score
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum, round

bronze_base = "abfss://bronze@escooterstorage123.dfs.core.windows.net"

# READ DATA
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{bronze_base}/cities.csv")

# TOTALS
total_population = df.agg(_sum("population_L").alias("total_pop")).collect()[0]["total_pop"]

total_tourists = df.agg(_sum("tourists_count_cr").alias("total_tour")).collect()[0]["total_tour"]

# PERCENTAGES
df = df.withColumn(
    "population_pct",
    (col("population_L") / total_population) * 100
)

df = df.withColumn(
    "tourist_pct",
    (col("tourists_count_cr") / total_tourists) * 100
)

# FINAL SCORE
df = df.withColumn(
    "city_score",
    round((col("population_pct") + col("tourist_pct")) / 2, 2)
)

# FINAL OUTPUT
final_df = df.select(
    "city",
    "population_pct",
    "tourist_pct",
    "city_score"
)

display(final_df)

# COMMAND ----------

temp_path = f"{bronze_base}/temp_silver_city_score"
final_path = f"{bronze_base}/silver_city_score.csv"

# KEEP ONLY REQUIRED COLUMNS
final_df = df.select("city", "city_score")

# WRITE TO TEMP LOCATION
final_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(temp_path)

# GET GENERATED FILE
files = dbutils.fs.ls(temp_path)
csv_file = [f.path for f in files if f.path.endswith(".csv")][0]

# MOVE TO FINAL FILE
dbutils.fs.mv(csv_file, final_path)

# CLEAN TEMP FOLDER
dbutils.fs.rm(temp_path, True)

# VERIFY
display(dbutils.fs.ls(bronze_base))

# COMMAND ----------

# MAGIC %md
# MAGIC silver_weather_score

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum, round

bronze_base = "abfss://bronze@escooterstorage123.dfs.core.windows.net"

# READ DATA
weather_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{bronze_base}/weather.csv")

# STEP 1: RAW WEATHER SCORE
weather_df = weather_df.withColumn(
    "weather_raw_score",
    col("temp_marks") + col("rain_marks")
)

# STEP 2: TOTAL WEATHER SCORE (ALL CITIES)
total_weather = weather_df.agg(
    _sum("weather_raw_score").alias("total")
).collect()[0]["total"]

# STEP 3: PERCENTAGE CONTRIBUTION
weather_df = weather_df.withColumn(
    "weather_score",
    round((col("weather_raw_score") / total_weather) * 100, 2)
)

# FINAL OUTPUT
weather_score_df = weather_df.select(
    "city",
    "weather_score"
)

display(weather_score_df)

# COMMAND ----------

temp_path = f"{bronze_base}/temp_silver_weather"
final_path = f"{bronze_base}/silver_weather_score.csv"

# WRITE TO TEMP
weather_score_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(temp_path)

# LIST FILES
files = dbutils.fs.ls(temp_path)

# PICK ONLY PART FILE (SAFE METHOD)
csv_file = [f.path for f in files if "part-" in f.path][0]

# MOVE FILE
dbutils.fs.mv(csv_file, final_path)

# CLEAN TEMP
dbutils.fs.rm(temp_path, True)

# VERIFY
display(dbutils.fs.ls(bronze_base))

# COMMAND ----------

# MAGIC %md
# MAGIC silver_transport_score

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum, round

bronze_base = "abfss://bronze@escooterstorage123.dfs.core.windows.net"

# READ DATA
transport_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{bronze_base}/transport.csv")

# STEP 1: RAW SCORE
transport_df = transport_df.withColumn(
    "transport_raw_score",
    (col("flight_pop") + col("rail_pop"))
)

# STEP 2: TOTAL ACROSS ALL CITIES
total_transport = transport_df.agg(
    _sum("transport_raw_score").alias("total")
).collect()[0]["total"]

# STEP 3: PERCENTAGE CONTRIBUTION
transport_df = transport_df.withColumn(
    "transport_score",
    round((col("transport_raw_score") / total_transport) * 100, 2)
)

# FINAL OUTPUT
transport_score_df = transport_df.select(
    "city",
    "transport_score"
)

display(transport_score_df)

# COMMAND ----------

temp_path = f"{bronze_base}/temp_silver_transport"
final_path = f"{bronze_base}/silver_transport_score.csv"

transport_score_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(temp_path)

files = dbutils.fs.ls(temp_path)
csv_file = [f.path for f in files if "part-" in f.path][0]

dbutils.fs.mv(csv_file, final_path)
dbutils.fs.rm(temp_path, True)

display(dbutils.fs.ls(bronze_base))

# COMMAND ----------

# MAGIC %md
# MAGIC POI

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum, round

bronze_base = "abfss://bronze@escooterstorage123.dfs.core.windows.net"

# READ DATA
poi_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{bronze_base}/poi.csv")

# STEP 1: RAW SCORE
poi_df = poi_df.withColumn(
    "poi_raw_score",
    col("office_count") +
    col("pg_count") +
    col("col_uni_count") +
    col("malls_count")
)

# STEP 2: TOTAL ACROSS ALL CITIES
total_poi = poi_df.agg(
    _sum("poi_raw_score").alias("total")
).collect()[0]["total"]

# STEP 3: PERCENTAGE CONTRIBUTION
poi_df = poi_df.withColumn(
    "poi_score",
    round((col("poi_raw_score") / total_poi) * 100, 2)
)

# FINAL OUTPUT
poi_score_df = poi_df.select(
    "city",
    "poi_score"
)

display(poi_score_df)

# COMMAND ----------

temp_path = f"{bronze_base}/temp_silver_poi"
final_path = f"{bronze_base}/silver_poi_score.csv"

poi_score_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(temp_path)

files = dbutils.fs.ls(temp_path)
csv_file = [f.path for f in files if "part-" in f.path][0]

dbutils.fs.mv(csv_file, final_path)
dbutils.fs.rm(temp_path, True)

display(dbutils.fs.ls(bronze_base))

# COMMAND ----------

# MAGIC %md
# MAGIC time

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum, round

bronze_base = "abfss://bronze@escooterstorage123.dfs.core.windows.net"

# READ DATA
time_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{bronze_base}/time_value.csv")

# STEP 1: RAW SCORE
time_df = time_df.withColumn(
    "time_raw_score",
    col("time_value") * 10
)

# STEP 2: TOTAL ACROSS ALL HOURS
total_time = time_df.agg(
    _sum("time_raw_score").alias("total")
).collect()[0]["total"]

# STEP 3: PERCENTAGE CONTRIBUTION
time_df = time_df.withColumn(
    "time_score",
    round((col("time_raw_score") / total_time) * 100, 2)
)

# FINAL OUTPUT
time_score_df = time_df.select(
    "hour",
    "time_score"
)

display(time_score_df)

# COMMAND ----------

temp_path = f"{bronze_base}/temp_silver_time"
final_path = f"{bronze_base}/silver_time_score.csv"

time_score_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(temp_path)

files = dbutils.fs.ls(temp_path)
csv_file = [f.path for f in files if "part-" in f.path][0]

dbutils.fs.mv(csv_file, final_path)
dbutils.fs.rm(temp_path, True)

display(dbutils.fs.ls(bronze_base))

# COMMAND ----------

# MAGIC %md
# MAGIC silver_safety_score

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum, round

bronze_base = "abfss://bronze@escooterstorage123.dfs.core.windows.net"

# READ DATA
safety_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{bronze_base}/safety.csv")

# STEP 1: RAW SCORE (your original logic)
safety_df = safety_df.withColumn(
    "safety_raw",
    (col("safety_index") - col("crime_rate") + 100) / 2
)

# STEP 2: TOTAL ACROSS ALL CITIES
total_safety = safety_df.agg(
    _sum("safety_raw").alias("total")
).collect()[0]["total"]

# STEP 3: PERCENTAGE CONTRIBUTION
safety_df = safety_df.withColumn(
    "safety_score",
    round((col("safety_raw") / total_safety) * 100, 2)
)

# FINAL OUTPUT
safety_score_df = safety_df.select(
    "city",
    "safety_score"
)

display(safety_score_df)

# COMMAND ----------

temp_path = f"{bronze_base}/temp_silver_safety"
final_path = f"{bronze_base}/silver_safety_score.csv"

# KEEP ONLY REQUIRED COLUMNS
final_df = safety_score_df.select("city", "safety_score")

# WRITE TO TEMP LOCATION
final_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(temp_path)

# GET GENERATED FILE
files = dbutils.fs.ls(temp_path)
csv_file = [f.path for f in files if "part-" in f.path][0]

# MOVE FILE TO FINAL LOCATION
dbutils.fs.mv(csv_file, final_path)

# CLEAN TEMP FOLDER
dbutils.fs.rm(temp_path, True)

# VERIFY
display(dbutils.fs.ls(bronze_base))

# COMMAND ----------

# DBTITLE 1,all in one
from pyspark.sql.functions import col

bronze_base = "abfss://bronze@escooterstorage123.dfs.core.windows.net"

# READ ALL CITY-LEVEL TABLES
city_df = spark.read.csv(f"{bronze_base}/silver_city_score.csv", header=True, inferSchema=True)
revenue_df = spark.read.csv(f"{bronze_base}/silver_revenue_score.csv", header=True, inferSchema=True)
weather_df = spark.read.csv(f"{bronze_base}/silver_weather_score.csv", header=True, inferSchema=True)
transport_df = spark.read.csv(f"{bronze_base}/silver_transport_score.csv", header=True, inferSchema=True)
poi_df = spark.read.csv(f"{bronze_base}/silver_poi_score.csv", header=True, inferSchema=True)
safety_df = spark.read.csv(f"{bronze_base}/silver_safety_score.csv", header=True, inferSchema=True)

# ENSURE CITY CLEAN (VERY IMPORTANT)
from pyspark.sql.functions import trim, lower

def clean(df):
    return df.withColumn("city", lower(trim(col("city"))))

city_df = clean(city_df)
revenue_df = clean(revenue_df)
weather_df = clean(weather_df)
transport_df = clean(transport_df)
poi_df = clean(poi_df)
safety_df = clean(safety_df)

# REMOVE DUPLICATES (SAFE JOIN)
city_df = city_df.dropDuplicates(["city"])
revenue_df = revenue_df.dropDuplicates(["city"])
weather_df = weather_df.dropDuplicates(["city"])
transport_df = transport_df.dropDuplicates(["city"])
poi_df = poi_df.dropDuplicates(["city"])
safety_df = safety_df.dropDuplicates(["city"])

# JOIN ALL TABLES
df = city_df \
    .join(revenue_df, "city", "left") \
    .join(weather_df, "city", "left") \
    .join(transport_df, "city", "left") \
    .join(poi_df, "city", "left") \
    .join(safety_df, "city", "left")

df = df.fillna(0)
# FINAL OUTPUT
silver_all_scores_df = df.select(
    "city",
    "city_score",
    "revenue_score",
    "weather_score",
    "transport_score",
    "poi_score",
    "safety_score"
)

display(silver_all_scores_df)

# COMMAND ----------

temp_path = f"{bronze_base}/temp_silver_all_scores"
final_path = f"{bronze_base}/silver_all_scores.csv"

# WRITE TO TEMP LOCATION
silver_all_scores_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(temp_path)

# GET GENERATED PART FILE
files = dbutils.fs.ls(temp_path)
csv_file = [f.path for f in files if "part-" in f.path][0]

# MOVE TO FINAL FILE
dbutils.fs.mv(csv_file, final_path)

# CLEAN TEMP FOLDER
dbutils.fs.rm(temp_path, True)

# VERIFY
display(dbutils.fs.ls(bronze_base))

# COMMAND ----------

