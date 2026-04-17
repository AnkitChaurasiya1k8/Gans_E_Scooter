# Databricks notebook source
bronze_base = "abfss://bronze@escooterstorage123.dfs.core.windows.net"

poi_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{bronze_base}/poi.csv")

display(poi_df)