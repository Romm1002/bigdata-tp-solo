# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, lag, corr, avg, lit, min, max, datediff, unix_timestamp
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Analyse des corrélations des événements sismiques") \
    .getOrCreate()

df_cleaned = spark.read.csv("hdfs://namenode:9000/dataset_sismique_cleaned/part-00000-65285987-7dc5-4062-8a48-eb2d74e6054f-c000.csv", header=True, inferSchema=True)

correlation_analysis_magnitude = df_cleaned.stat.corr("tension entre plaque", "magnitude")
print(f"Corrélation entre la tension entre plaque et les séismes : {correlation_analysis_magnitude}")

threshold_major_event = 4.0  

df_events = df_cleaned.filter(col("secousse") == True)

window_spec = Window.orderBy("date")

df_cleaned = df_events.withColumns({"date_previous": lag("date", 1).over(window_spec), "magnitude_previous": lag("magnitude", 1).over(window_spec), "tension_previous": lag("tension entre plaque", 1).over(window_spec), })
df_major_events = df_cleaned.filter(col("magnitude") >= threshold_major_event)
df_major_events.show()

avg_magnitude_previous = df_major_events.select(avg("magnitude_previous").alias("avg_magnitude_previous")).collect()[0][0]

median_magnitude_previous = df_major_events.approxQuantile("magnitude_previous", [0.5], 0.001)[0]

print(f"Moyenne de la magnitude des événements précédents un séisme majeur: {avg_magnitude_previous}")
print(f"Médiane de la magnitude des événements précédents un séisme majeur: {median_magnitude_previous}")

correlation_analysis = df_cleaned.stat.corr("magnitude_previous", "magnitude")
print(f"Corrélation entre la magnitude des événements précédents les séismes majeurs et la magnitude des séismes majeurs: {correlation_analysis}")


df_date_diff = df_cleaned.withColumn(
    "date_diff_seconds", 
    (unix_timestamp("date") - unix_timestamp("date_previous")) / 60
)

df_date_diff.agg(
    min("date_diff_seconds").alias("min_date_diff_minutes"),
    max("date_diff_seconds").alias("max_date_diff_minutes")
).show()
avg_magnitude_previous = df_date_diff.select(avg("date_diff_seconds").alias("avg_date_diff_seconds")).collect()[0][0]

median_magnitude_previous = df_date_diff.approxQuantile("date_diff_seconds", [0.5], 0.001)[0]

print(f"Moyenne du temps entre l'événement précédent un séisme majeur et le séisme majeur: {avg_magnitude_previous}")
print(f"Médiane du temps entre l'événement précédent un séisme majeur et le séisme majeur: {median_magnitude_previous}")
spark.stop()
