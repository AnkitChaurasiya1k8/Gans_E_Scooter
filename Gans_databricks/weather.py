# Databricks notebook source
import requests
import pandas as pd
import time

API_KEY = "566e60c94faab196761b02f860638862"

file_path = "abfss://bronze@escooterstorage123.dfs.core.windows.net/cities.csv"

cities_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path)

cities = [row["city"] for row in cities_df.select("city").collect()]

all_weather = []

for city in cities:
    
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    
    response = requests.get(url)
    data = response.json()
    
    if "main" not in data:
        print(f"Error for {city}: {data}")
        continue
    
    temp = data["main"]["temp"]
    rain_mm = data.get("rain", {}).get("1h", 0)
    
    # TEMP MARKS
    if 20 <= temp <= 30:
        temp_marks = 50
    elif temp < 20:
        temp_marks = 25
    else:
        temp_marks = 10
    
    # RAIN MARKS
    if rain_mm == 0:
        rain_marks = 50
    elif rain_mm <= 2:
        rain_marks = 40
    elif rain_mm <= 5:
        rain_marks = 25
    elif rain_mm <= 10:
        rain_marks = 10
    else:
        rain_marks = 0
    
    all_weather.append({
        "city": city,
        "temperature": temp,
        "temp_marks": temp_marks,
        "rainfall_mm": rain_mm,
        "rain_marks": rain_marks
    })
    
    time.sleep(1)

df_weather = pd.DataFrame(all_weather)
weather_df = spark.createDataFrame(df_weather)

display(weather_df)

# COMMAND ----------

temp_path = "abfss://bronze@escooterstorage123.dfs.core.windows.net/temp_weather"
final_path = "abfss://bronze@escooterstorage123.dfs.core.windows.net/weather.csv"

weather_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(temp_path)

files = dbutils.fs.ls(temp_path)
csv_file = [f.path for f in files if f.path.endswith(".csv")][0]

dbutils.fs.mv(csv_file, final_path)
dbutils.fs.rm(temp_path, True)

display(dbutils.fs.ls("abfss://bronze@escooterstorage123.dfs.core.windows.net/"))