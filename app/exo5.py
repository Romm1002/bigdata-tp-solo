# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Analyse des corrélations des événements sismiques") \
    .getOrCreate()

df_cleaned = spark.read.csv("hdfs://namenode:9000/dataset_sismique_cleaned/part-00000-cfda7cb0-2a20-4895-b8c9-9897dad031ed-c000.csv", header=True, inferSchema=True)

correlation_score = df_cleaned.stat.corr("tension entre plaque", "magnitude")

print(f"Score de corrélation entre magnitude et tension entre plaque: {correlation_score}")

spark.stop()
