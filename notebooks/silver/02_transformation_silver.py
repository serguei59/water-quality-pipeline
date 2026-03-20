# Databricks notebook source
# MAGIC %md
# MAGIC # Couche Silver - Nettoyage et Transformation
# MAGIC
# MAGIC Ce notebook transforme les données brutes de la couche Bronze en données
# MAGIC nettoyées, dédupliquées et enrichies dans la couche Silver.
# MAGIC
# MAGIC **Tables créées :**
# MAGIC - `silver_stations` : Stations géolocalisées et validées
# MAGIC - `silver_mesures` : Mesures nettoyées avec unités standardisées
# MAGIC - `silver_conformite` : Évaluation de conformité aux normes européennes

# COMMAND ----------

import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, DateType, TimestampType, StringType, IntegerType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

DATABASE_NAME = "water_quality"
SILVER_PATH = "/mnt/delta/silver"

spark.sql(f"USE {DATABASE_NAME}")

# Seuils de conformité européenne (Directive 2000/60/CE et directive eau potable)
SEUILS_CONFORMITE = {
    "1340": {"nom": "Nitrates", "seuil_alerte": 25.0, "seuil_max": 50.0, "unite": "mg/L"},
    "1350": {"nom": "Nitrites", "seuil_alerte": 0.05, "seuil_max": 0.1, "unite": "mg/L"},
    "1433": {"nom": "Phosphore total", "seuil_alerte": 0.1, "seuil_max": 0.2, "unite": "mg/L"},
    "1302": {"nom": "pH", "seuil_alerte": None, "seuil_max": None, "unite": "unité pH", "plage_min": 6.5, "plage_max": 9.0},
    "1301": {"nom": "Température", "seuil_alerte": 20.0, "seuil_max": 25.0, "unite": "°C"},
    "1311": {"nom": "Oxygène dissous", "seuil_alerte": 5.0, "seuil_max": None, "unite": "mg/L", "sens": "min"},
    "1841": {"nom": "Escherichia coli", "seuil_alerte": 1000.0, "seuil_max": 2000.0, "unite": "UFC/100mL"},
}

# Codes de remarques Hub'Eau
CODES_REMARQUES = {
    "1": "Valeur normale",
    "2": "Valeur inférieure au seuil de quantification",
    "3": "Valeur inférieure au seuil de détection",
    "4": "Valeur supérieure au seuil de saturation",
    "5": "Valeur douteuse",
    "6": "Valeur non réalisée",
    "7": "Valeur non significative",
    "10": "Valeur réestimée",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Transformation silver_stations

# COMMAND ----------

logger.info("=== TRANSFORMATION STATIONS ===")

df_bronze_stations = spark.table(f"{DATABASE_NAME}.bronze_stations")

df_silver_stations = (
    df_bronze_stations
    # Cast des types
    .withColumn("longitude", F.col("longitude").cast(DoubleType()))
    .withColumn("latitude", F.col("latitude").cast(DoubleType()))
    .withColumn("coordonnee_x", F.col("coordonnee_x").cast(DoubleType()))
    .withColumn("coordonnee_y", F.col("coordonnee_y").cast(DoubleType()))
    .withColumn("altitude_ref_alti", F.col("altitude_ref_alti").cast(DoubleType()))
    .withColumn("date_ouverture_station", F.to_date("date_ouverture_station", "yyyy-MM-dd"))
    .withColumn("date_fermeture_station", F.to_date("date_fermeture_station", "yyyy-MM-dd"))

    # Nettoyage des chaînes
    .withColumn("nom_station", F.trim(F.col("nom_station")))
    .withColumn("nom_commune", F.trim(F.col("nom_commune")))
    .withColumn("nom_departement", F.trim(F.col("nom_departement")))

    # Filtre : stations avec coordonnées valides et en France métropolitaine
    .filter(F.col("latitude").between(41.0, 51.5))
    .filter(F.col("longitude").between(-5.5, 10.0))
    .filter(F.col("code_station").isNotNull())

    # Dédoublonnage
    .dropDuplicates(["code_station"])

    # Colonne statut station
    .withColumn("station_active",
        F.when(F.col("date_fermeture_station").isNull(), True).otherwise(False))

    # Métadonnées Silver
    .withColumn("_silver_timestamp", F.current_timestamp())
    .select(
        "code_station", "nom_station", "uri_station",
        "code_commune", "nom_commune",
        "code_departement", "nom_departement",
        "code_region", "nom_region",
        "longitude", "latitude", "coordonnee_x", "coordonnee_y",
        "altitude_ref_alti",
        "code_masse_eau", "nom_masse_eau",
        "date_ouverture_station", "date_fermeture_station",
        "station_active", "finalite_station", "type_entite_hydro",
        "_silver_timestamp"
    )
)

path = f"{SILVER_PATH}/silver_stations"
(df_silver_stations.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("path", path)
    .saveAsTable(f"{DATABASE_NAME}.silver_stations"))

logger.info(f"silver_stations : {df_silver_stations.count():,} stations valides")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transformation silver_mesures

# COMMAND ----------

logger.info("=== TRANSFORMATION MESURES ===")

df_bronze_analyses = spark.table(f"{DATABASE_NAME}.bronze_analyses")

# Mapping des codes remarques
codes_remarques_map = F.create_map(
    *[item for pair in [(F.lit(k), F.lit(v)) for k, v in CODES_REMARQUES.items()] for item in pair]
)

df_silver_mesures = (
    df_bronze_analyses

    # Cast des types
    .withColumn("resultat", F.col("resultat").cast(DoubleType()))
    .withColumn("limite_detection", F.col("limite_detection").cast(DoubleType()))
    .withColumn("limite_quantification", F.col("limite_quantification").cast(DoubleType()))
    .withColumn("date_prelevement", F.to_date("date_prelevement", "yyyy-MM-dd"))
    .withColumn("code_remarque", F.col("code_remarque").cast(StringType()))

    # Suppression des lignes sans résultat ni code station
    .filter(F.col("code_station").isNotNull())
    .filter(F.col("code_parametre").isNotNull())
    .filter(F.col("date_prelevement").isNotNull())

    # Enrichissement code remarque
    .withColumn("libelle_remarque",
        F.coalesce(codes_remarques_map[F.col("code_remarque")], F.lit("Inconnu")))

    # Gestion des valeurs sous seuil de détection
    .withColumn("resultat_corrige",
        F.when(F.col("code_remarque") == "3",
               F.col("limite_detection") / 2)
         .when(F.col("code_remarque") == "2",
               F.col("limite_quantification") / 2)
         .otherwise(F.col("resultat")))

    # Flag valeur valide (codes 1 et 10 = valeurs mesurées exploitables)
    .withColumn("valeur_valide",
        F.col("code_remarque").isin(["1", "10"]))

    # Extraction temporelle
    .withColumn("annee", F.year("date_prelevement"))
    .withColumn("mois", F.month("date_prelevement"))
    .withColumn("trimestre", F.quarter("date_prelevement"))

    # Dédoublonnage sur clé métier
    .dropDuplicates(["code_analyse"])

    # Métadonnées Silver
    .withColumn("_silver_timestamp", F.current_timestamp())

    .select(
        "code_analyse", "code_station", "nom_station",
        "code_parametre", "nom_parametre",
        "code_unite", "nom_unite", "symbole_unite",
        "date_prelevement", "heure_prelevement",
        "annee", "mois", "trimestre",
        "resultat", "resultat_corrige",
        "code_remarque", "libelle_remarque", "valeur_valide",
        "limite_detection", "limite_quantification",
        "code_qualification", "libelle_qualification",
        "code_support", "nom_support",
        "code_fraction_analysee", "nom_fraction_analysee",
        "code_methode", "nom_methode",
        "_silver_timestamp"
    )
)

path = f"{SILVER_PATH}/silver_mesures"
(df_silver_mesures.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("path", path)
    .saveAsTable(f"{DATABASE_NAME}.silver_mesures"))

logger.info(f"silver_mesures : {df_silver_mesures.count():,} mesures")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Transformation silver_conformite

# COMMAND ----------

logger.info("=== CREATION SILVER CONFORMITE ===")

# Seuils en DataFrame pour jointure
seuils_rows = [
    (code, info["nom"], info.get("seuil_alerte"), info.get("seuil_max"),
     info.get("plage_min"), info.get("plage_max"), info.get("unite"), info.get("sens", "max"))
    for code, info in SEUILS_CONFORMITE.items()
]

seuils_schema = ["code_parametre", "nom_parametre_ref", "seuil_alerte", "seuil_max",
                 "plage_min", "plage_max", "unite_ref", "sens"]
df_seuils = spark.createDataFrame(seuils_rows, seuils_schema)

df_silver_conformite = (
    df_silver_mesures
    .filter(F.col("valeur_valide"))
    .join(df_seuils, "code_parametre", "inner")
    .withColumn("statut_conformite",
        F.when(
            (F.col("sens") == "max") & F.col("seuil_max").isNotNull() &
            (F.col("resultat_corrige") > F.col("seuil_max")),
            F.lit("NON_CONFORME")
        ).when(
            (F.col("sens") == "max") & F.col("seuil_alerte").isNotNull() &
            (F.col("resultat_corrige") > F.col("seuil_alerte")),
            F.lit("ALERTE")
        ).when(
            (F.col("sens") == "min") & F.col("seuil_alerte").isNotNull() &
            (F.col("resultat_corrige") < F.col("seuil_alerte")),
            F.lit("ALERTE")
        ).when(
            F.col("plage_min").isNotNull() &
            ((F.col("resultat_corrige") < F.col("plage_min")) |
             (F.col("resultat_corrige") > F.col("plage_max"))),
            F.lit("NON_CONFORME")
        ).otherwise(F.lit("CONFORME"))
    )
    .withColumn("_silver_timestamp", F.current_timestamp())
    .select(
        "code_analyse", "code_station", "nom_station",
        "code_parametre", "nom_parametre_ref",
        "date_prelevement", "annee", "mois", "trimestre",
        "resultat_corrige", "unite_ref",
        "seuil_alerte", "seuil_max",
        "statut_conformite",
        "_silver_timestamp"
    )
)

path = f"{SILVER_PATH}/silver_conformite"
(df_silver_conformite.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("path", path)
    .saveAsTable(f"{DATABASE_NAME}.silver_conformite"))

logger.info(f"silver_conformite : {df_silver_conformite.count():,} évaluations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Contrôles qualité

# COMMAND ----------

print("\n" + "="*60)
print("CONTROLES QUALITE - COUCHE SILVER")
print("="*60)

# Taux de valeurs valides
total = df_silver_mesures.count()
valides = df_silver_mesures.filter(F.col("valeur_valide")).count()
print(f"\nTaux de valeurs valides : {valides/total*100:.1f}% ({valides:,}/{total:,})")

# Distribution des statuts de conformité
print("\nDistribution des statuts de conformité :")
df_silver_conformite.groupBy("statut_conformite").count().orderBy("count", ascending=False).show()

# Paramètres les plus mesurés
print("Top 10 paramètres les plus mesurés :")
df_silver_mesures.groupBy("code_parametre", "nom_parametre").count() \
    .orderBy("count", ascending=False).show(10)

# Couverture temporelle
print("Couverture temporelle :")
df_silver_mesures.agg(
    F.min("date_prelevement").alias("date_min"),
    F.max("date_prelevement").alias("date_max"),
    F.countDistinct("annee").alias("nb_annees")
).show()

print("="*60)
print(f"Transformation Silver terminée : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
