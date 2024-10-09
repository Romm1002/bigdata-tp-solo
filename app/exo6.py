# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, min, count, window

spark = SparkSession.builder \
    .appName("Analyse des tendances de l'activité sismique") \
    .getOrCreate()

df_cleaned = spark.read.csv("hdfs://namenode:9000/dataset_sismique_cleaned/part-00000-cfda7cb0-2a20-4895-b8c9-9897dad031ed-c000.csv", header=True, inferSchema=True)

threshold = 0
events = df_cleaned.filter(col("magnitude") > threshold)

activity_hourly = events.groupBy(window(col("date"), "1 hour")) \
    .agg(
        count("magnitude").alias("nombre_de_secousses"),
        max("magnitude").alias("magnitude_max"),
        min("magnitude").alias("magnitude_min"),
    ) \
    .withColumn("amplitude", col("magnitude_max") - col("magnitude_min"))

print("Analyse de l'activité sismique par heure:")
activity_hourly.orderBy("window.start").show(truncate=False)

activity_daily = events.groupBy(window(col("date"), "1 day")) \
    .agg(
        count("magnitude").alias("nombre_de_secousses"),
        max("magnitude").alias("magnitude_max"),
        min("magnitude").alias("magnitude_min"),
    ) \
    .withColumn("amplitude", col("magnitude_max") - col("magnitude_min"))

print("Analyse de l'activité sismique par jour:")
activity_daily.orderBy("window.start").show(truncate=False)

spark.stop()
