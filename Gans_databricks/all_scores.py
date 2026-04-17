# Databricks notebook source
base_path = "abfss://bronze@escooterstorage123.dfs.core.windows.net/"

cities = spark.read.csv(base_path + "silver_cities_score.csv", header=True, inferSchema=True)
weather = spark.read.csv(base_path + "silver_weather_score.csv", header=True, inferSchema=True)
transport = spark.read.csv(base_path + "silver_transport_score.csv", header=True, inferSchema=True)
poi = spark.read.csv(base_path + "silver_poi_score.csv", header=True, inferSchema=True)
safety = spark.read.csv(base_path + "silver_safety_score.csv", header=True, inferSchema=True)
revenue = spark.read.csv(base_path + "silver_revenue_score.csv", header=True, inferSchema=True)
time_df = spark.read.csv(base_path + "silver_time_score.csv", header=True, inferSchema=True)

display(revenue)

# COMMAND ----------

# READ EXPECTED DEMAND (EXISTING FILE)
expected_df = spark.read.csv(base_path + "final_expected_demand.csv", header=True, inferSchema=True)

# GET TIME SCORE VALUE
time_score_value = time_df.collect()[0]["time_score"]

# JOIN ALL TABLES
df = cities \
    .join(weather, "city", "left") \
    .join(transport, "city", "left") \
    .join(poi, "city", "left") \
    .join(safety, "city", "left") \
    .join(revenue, "city", "left") \
    .join(expected_df, "city", "left")

# ADD TIME SCORE
df = df.withColumn("time_score", lit(time_score_value))

# FINAL TABLE STRUCTURE (MERGED)
silver_all_scores_df = df.select(
    "city",
    "city_score",
    "transport_score",
    "poi_score",
    "revenue_score",
    "weather_score",
    "safety_score",
    "time_score",
    "expected_demand"
)

# SAVE AS TABLE
silver_all_scores_df.write.mode("overwrite").saveAsTable("silver_all_scores")

# SHOW RESULT
display(silver_all_scores_df)