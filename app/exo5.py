# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, lag, corr
from pyspark.sql.window import Window

# Initialiser une session Spark
spark = SparkSession.builder \
    .appName("Analyse des corrélations des événements sismiques") \
    .getOrCreate()

# Charger les données nettoyées depuis HDFS
df_cleaned = spark.read.csv("hdfs://namenode:9000/dataset_sismique_cleaned/part-00000-cfda7cb0-2a20-4895-b8c9-9897dad031ed-c000.csv", header=True, inferSchema=True)

# 1. Définir un seuil pour détecter les événements sismiques importants
threshold_major_event = 5.0  # Seuil pour définir un séisme majeur

# 2. Créer une fenêtre temporelle basée sur la date des événements
# Cette fenêtre ne spécifie pas de frame, juste un ordre par la date
window_spec = Window.orderBy("date")

# 3. Utiliser la fonction lag() pour obtenir la magnitude de l'événement précédent
df_cleaned = df_cleaned.withColumn("magnitude_previous", lag("magnitude", 1).over(window_spec))

# 4. Filtrer pour récupérer uniquement les événements majeurs et leurs précédents
df_major_events = df_cleaned.filter(col("magnitude") >= threshold_major_event)

# 5. Calculer la corrélation entre la magnitude des événements précédents et celle des événements majeurs
correlation_analysis = df_cleaned.stat.corr("magnitude_previous", "magnitude")

print(f"Corrélation entre la magnitude des événements précédents et les séismes majeurs: {correlation_analysis}")

# 6. Visualiser les événements précédant les séismes majeurs et leurs magnitudes
df_major_events.select("date", "magnitude", "magnitude_previous").show()

# 7. Identifier des séquences de secousses avant les événements majeurs
df_sequences = df_cleaned.filter(col("magnitude_previous").isNotNull()) \
    .filter(col("magnitude_previous") < threshold_major_event)  # Filtrer les petits séismes avant des grands

print("Séquences de petits séismes précédant des séismes majeurs:")
df_sequences.show()

# Stopper la session Spark
spark.stop()
