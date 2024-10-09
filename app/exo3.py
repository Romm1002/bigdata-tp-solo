# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import BooleanType, FloatType, TimestampType

# Initialiser une session Spark
spark = SparkSession.builder \
    .appName("Nettoyage des données sismiques") \
    .getOrCreate()

# 1. Charger les données depuis HDFS
df = spark.read.csv("hdfs://namenode:9000/dataset_sysmique_1.csv", header=True, inferSchema=True)

# Afficher les premières lignes pour vérifier le contenu
df.head()

# Vérifier le schéma du DataFrame
df.printSchema()

# 2. Suppression des valeurs aberrantes et correction des erreurs
# Supprimer les valeurs aberrantes pour 'magnitude' (> 10 ou < 0) et 'tension entre plaque' (< 0)
df_cleaned = df.filter((col("magnitude") >= 0) & (col("magnitude") <= 10) & (col("tension entre plaque") >= 0))

# 3. Remplacer les valeurs manquantes
# Remplacer les valeurs manquantes par des valeurs par défaut (0 pour la magnitude et la tension entre plaques)
df_cleaned = df_cleaned.na.fill({"magnitude": 0.0, "tension entre plaque": 0.0})

# 4. Corriger les types de données si nécessaire
df_cleaned = df_cleaned \
    .withColumn("date", col("date").cast(TimestampType())) \
    .withColumn("secousse", col("secousse").cast(BooleanType())) \
    .withColumn("magnitude", col("magnitude").cast(FloatType())) \
    .withColumn("tension entre plaque", col("tension entre plaque").cast(FloatType()))

# Afficher les données nettoyées
df_cleaned.show()

# Vérifier le nouveau schéma
df_cleaned.printSchema()

# 5. Enregistrer les données nettoyées dans HDFS
df_cleaned.write.csv("hdfs://namenode:9000/dataset_sismique_cleaned", header=True)

# Stopper la session Spark
spark.stop()
