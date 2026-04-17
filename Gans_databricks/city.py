# Databricks notebook source
# MAGIC %pip install lxml

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import requests
from bs4 import BeautifulSoup

URL = "https://en.wikipedia.org/wiki/List_of_cities_in_India_by_population"

headers = {"User-Agent": "Mozilla/5.0"}

response = requests.get(URL, headers=headers)
soup = BeautifulSoup(response.text, "lxml")

table = soup.find_all("table", {"class": "wikitable"})[0]
rows = table.find_all("tr")

pop_data = []

for row in rows[1:]:
    cols = row.find_all("td")
    
    if len(cols) > 4:
        city = cols[1].text.strip().split("(")[0].strip().lower()
        population = cols[3].text.strip().replace(",", "").replace(".", "")
        
        if population.isdigit():
            pop_data.append((city, int(population)))

# Top 15
pop_data = pop_data[:15]

print(pop_data)

# COMMAND ----------

# DBTITLE 1,save population table
spark_df = spark.createDataFrame(pop_data, ["city", "population"])

spark_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("most_pop_cities")

# COMMAND ----------

# DBTITLE 1,load tourism data
from pyspark.sql.functions import col, lower

file_path = "abfss://bronze@escooterstorage123.dfs.core.windows.net/tourism_cities.csv"

tour_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path)

# Standardize city
tour_df = tour_df.withColumn("city", lower(col("city")))

display(tour_df)

# COMMAND ----------

# DBTITLE 1,save tourism table
tour_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("tourism_city")

# COMMAND ----------

# DBTITLE 1,top tourism cities
from pyspark.sql.functions import col

top_tour_df = spark.table("tourism_city") \
    .orderBy(col("tourists_count_cr").desc()) \
    .limit(5)

tour_list = [row["city"] for row in top_tour_df.collect()]

print("Top Tourism Cities:", tour_list)

# COMMAND ----------

# DBTITLE 1,final city list
pop_list = [city for city, _ in pop_data]

final_cities = []

# Add tourism cities
for city in tour_list:
    if city not in final_cities:
        final_cities.append(city)

# Fill from population
for city in pop_list:
    if city not in final_cities:
        final_cities.append(city)
    if len(final_cities) == 10:
        break

print("Final Cities:", final_cities)

final_df = spark.createDataFrame([(city,) for city in final_cities], ["city"])

final_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("final_city_list")

display(final_df)

# COMMAND ----------

# DBTITLE 1,final join
from pyspark.sql.functions import col, floor, lower

# Load tables
city_df = spark.table("final_city_list")
pop_df = spark.table("most_pop_cities")
tour_df = spark.table("tourism_city")

# Standardize city names
city_df = city_df.withColumn("city", lower(col("city")))
pop_df = pop_df.withColumn("city", lower(col("city")))
tour_df = tour_df.withColumn("city", lower(col("city")))

# Join
cities_df = city_df \
    .join(pop_df, "city", "left") \
    .join(tour_df, "city", "left") \
    .withColumn("population_L", floor(col("population") / 100000)) \
    .drop("population") \
    .fillna({
        "population_L": 15,
        "tourists_count_cr": 1
    })

# Save
cities_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable("cities")

display(cities_df)

# COMMAND ----------

temp_path = "abfss://bronze@escooterstorage123.dfs.core.windows.net/temp_cities"
final_path = "abfss://bronze@escooterstorage123.dfs.core.windows.net/cities.csv"

# Step 1: Write as single partition
cities_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(temp_path)

# Step 2: Get the CSV file
files = dbutils.fs.ls(temp_path)
csv_file = [f.path for f in files if f.path.endswith(".csv")][0]

# Step 3: Move and rename
dbutils.fs.mv(csv_file, final_path)

# Step 4: Delete temp folder
dbutils.fs.rm(temp_path, True)

# Step 5: Verify
display(dbutils.fs.ls("abfss://bronze@escooterstorage123.dfs.core.windows.net/"))

# COMMAND ----------

file_path = "abfss://bronze@escooterstorage123.dfs.core.windows.net/cities.csv"

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path)

display(df)

# COMMAND ----------

