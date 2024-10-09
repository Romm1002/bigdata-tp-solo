# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, window, when, max, min

spark = SparkSession.builder \
    .appName("Analyse préliminaire des données sismiques") \
    .getOrCreate()

df_cleaned = spark.read.csv("hdfs://namenode:9000/dataset_sismique_cleaned/part-00000-cfda7cb0-2a20-4895-b8c9-9897dad031ed-c000.csv", header=True, inferSchema=True)

threshold = 1.0
events = df_cleaned.filter(col("magnitude") >= threshold)
print("Événements sismiques détectés:")
events.show()

activity = events.groupBy(window(col("date"), "1 hour")) \
    .agg(
        max("magnitude").alias("magnitude_max"),
        min("magnitude").alias("magnitude_min"),
    ) \
    .withColumn("amplitude", col("magnitude_max") - col("magnitude_min"))
print("Analyse de l'activité sismique par heure:")
activity.orderBy("window.start").show()

threshold2 = 5.0
significant_activity = activity.filter(col("amplitude") >= threshold2)
print("Périodes d'activité sismique importante:")
significant_activity.show()

spark.stop()
