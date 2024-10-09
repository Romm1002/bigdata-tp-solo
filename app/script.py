# 
# Chargement du fichier dans l'HDFS
#
# hadoop fs -put /myhadoop/dataset_sysmique.csv /


# 
# EXERCICE 3
# 
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
df_cleaned.write.csv("hdfs://namenode:9000/dataset_sysmique_cleaned.csv", header=True)

# Stopper la session Spark
spark.stop()


# 
# EXERCICE 4
# 
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, count, sum, window

# # Initialiser une session Spark
# spark = SparkSession.builder \
#     .appName("Analyse préliminaire des données sismiques") \
#     .getOrCreate()

# # Charger les données nettoyées depuis HDFS
# df_cleaned = spark.read.csv("hdfs://namenode:9000/user/hadoop/data/dataset_sysmique_cleaned.csv", header=True, inferSchema=True)

# # 1. Calculer l'amplitude des signaux
# # Dans ce cas, l'amplitude peut être prise comme valeur absolue de la magnitude
# df_amplitude = df_cleaned.withColumn("amplitude", col("magnitude"))

# # 2. Définir un seuil pour détecter les événements sismiques importants
# threshold = 1.0  # Seuil à ajuster en fonction des besoins

# # 3. Identifier les événements sismiques importants
# events = df_amplitude.filter(col("amplitude") >= threshold)

# # Afficher les événements sismiques détectés
# print("Événements sismiques détectés:")
# events.show()

# # 4. Analyser les périodes d'activité sismique
# # Groupement par période (par minute) et calcul de l'amplitude totale
# activity_analysis = df_amplitude.groupBy(window(col("date"), "1 minute")) \
#     .agg(
#         count("secousse").alias("nombre_de_secousses"),
#         sum("amplitude").alias("amplitude_totale")
#     ) \
#     .filter(col("nombre_de_secousses") > 0)  # Filtrer les périodes avec des secousses

# # Afficher l'analyse de l'activité sismique
# print("Analyse de l'activité sismique par minute:")
# activity_analysis.show()

# # 5. Identifier les périodes d'activité sismique importante
# # Définir un seuil pour l'activité sismique (par exemple, amplitude totale)
# activity_threshold = 5.0  # Seuil à ajuster

# significant_activity = activity_analysis.filter(col("amplitude_totale") >= activity_threshold)

# # Afficher les périodes d'activité sismique importante
# print("Périodes d'activité sismique importante:")
# significant_activity.show()

# # Stopper la session Spark
# spark.stop()

