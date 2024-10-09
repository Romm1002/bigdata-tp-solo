# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import BooleanType, FloatType, TimestampType

spark = SparkSession.builder \
    .appName("Nettoyage des données sismiques") \
    .getOrCreate()

df = spark.read.csv("hdfs://namenode:9000/dataset_sysmique_1.csv", header=True, inferSchema=True)

df_cleaned = df.filter((col("magnitude") >= 0) & (col("magnitude") <= 10) & (col("tension entre plaque") >= 0))
df_cleaned = df_cleaned.na.fill({"magnitude": 0.0, "tension entre plaque": 0.0})
df_cleaned = df_cleaned \
    .withColumn("date", col("date").cast(TimestampType())) \
    .withColumn("secousse", col("secousse").cast(BooleanType())) \
    .withColumn("magnitude", col("magnitude").cast(FloatType())) \
    .withColumn("tension entre plaque", col("tension entre plaque").cast(FloatType()))

df_cleaned.write.csv("hdfs://namenode:9000/dataset_sismique_cleaned", header=True)

spark.stop()
