# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, window

spark = SparkSession.builder \
    .appName("Analyse préliminaire des données sismiques") \
    .getOrCreate()

df_cleaned = spark.read.csv("hdfs://namenode:9000/dataset_sismique_cleaned/part-00000-cfda7cb0-2a20-4895-b8c9-9897dad031ed-c000.csv", header=True, inferSchema=True)

threshold = 1.0
events = df_cleaned.filter(col("magnitude") >= threshold)
print("Événements sismiques détectés:")
events.show()

activity = df_cleaned.groupBy(window(col("date"), "1 minute")) \
    .agg(
        count("magnitude").alias("nombre_de_secousses"),
        sum("magnitude").alias("magnitude_totale")
    ) \
    .filter(col("nombre_de_secousses") > 0)
print("Analyse de l'activité sismique par minute:")
activity.show()

threshold2 = 5.0
significant_activity = activity.filter(col("magnitude_totale") >= threshold2)
print("Périodes d'activité sismique importante:")
significant_activity.show()

spark.stop()
