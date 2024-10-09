# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, window

# Initialiser une session Spark
spark = SparkSession.builder \
    .appName("Analyse préliminaire des données sismiques") \
    .getOrCreate()

# Charger les données nettoyées depuis HDFS
df_cleaned = spark.read.csv("hdfs://namenode:9000/dataset_sismique_cleaned/part-00000-cfda7cb0-2a20-4895-b8c9-9897dad031ed-c000.csv", header=True, inferSchema=True)

# 1. Définir un seuil pour détecter les événements sismiques importants
threshold = 1.0

# 2. Identifier les événements sismiques importants
events = df_cleaned.filter(col("magnitude") >= threshold)

# Afficher les événements sismiques détectés
print("Événements sismiques détectés:")
events.show()

# 3. Analyser les périodes d'activité sismique
# Groupement par période (par minute) et calcul de la magnitude totale
activity_analysis = df_cleaned.groupBy(window(col("date"), "1 minute")) \
    .agg(
        count("magnitude").alias("nombre_de_secousses"),
        sum("magnitude").alias("magnitude_totale")
    ) \
    .filter(col("nombre_de_secousses") > 0)  # Filtrer les périodes avec des secousses

# Afficher l'analyse de l'activité sismique
print("Analyse de l'activité sismique par minute:")
activity_analysis.show()

# 4. Identifier les périodes d'activité sismique importante
# Définir un seuil pour l'activité sismique (par exemple, magnitude totale)
activity_threshold = 5.0

significant_activity = activity_analysis.filter(col("magnitude_totale") >= activity_threshold)

# Afficher les périodes d'activité sismique importante
print("Périodes d'activité sismique importante:")
significant_activity.show()

# Stopper la session Spark
spark.stop()
