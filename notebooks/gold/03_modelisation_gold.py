# Databricks notebook source
# MAGIC %md
# MAGIC # Couche Gold - Modélisation Analytique
# MAGIC
# MAGIC Ce notebook construit les tables analytiques finales (star schema) pour
# MAGIC les analyses et dashboards.
# MAGIC
# MAGIC **Tables de dimension :**
# MAGIC - `dim_stations` : Dimension géographique
# MAGIC - `dim_parametres` : Dimension des paramètres
# MAGIC - `dim_temps` : Dimension temporelle
# MAGIC
# MAGIC **Tables de faits :**
# MAGIC - `fact_mesures_qualite` : Fait principal des mesures
# MAGIC - `fact_conformite_normes` : Conformité aux normes de potabilité
# MAGIC
# MAGIC **Tables agrégées :**
# MAGIC - `agg_qualite_departement` : KPIs par département
# MAGIC - `agg_evolution_temporelle` : Tendances annuelles et saisonnières
# MAGIC - `agg_alertes_parametres` : Dépassements de seuils réglementaires

# COMMAND ----------

import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, DateType, StringType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

DATABASE_NAME = "water_quality"
GOLD_PATH = "/mnt/delta/gold"

spark.sql(f"USE {DATABASE_NAME}")

def save_gold(df, table_name: str):
    """Sauvegarde une table dans la couche Gold au format Delta."""
    path = f"{GOLD_PATH}/{table_name}"
    (df.withColumn("_gold_timestamp", F.current_timestamp())
       .write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .option("path", path)
       .saveAsTable(f"{DATABASE_NAME}.{table_name}"))
    count = spark.table(f"{DATABASE_NAME}.{table_name}").count()
    logger.info(f"{table_name} : {count:,} enregistrements")
    return count

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Chargement des tables Silver

# COMMAND ----------

df_stations = spark.table(f"{DATABASE_NAME}.silver_stations")
df_mesures = spark.table(f"{DATABASE_NAME}.silver_mesures")
df_conformite = spark.table(f"{DATABASE_NAME}.silver_conformite")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tables de Dimension

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 dim_stations

# COMMAND ----------

logger.info("=== DIM_STATIONS ===")

dim_stations = (
    df_stations
    .select(
        F.monotonically_increasing_id().alias("station_sk"),
        "code_station",
        "nom_station",
        "code_commune", "nom_commune",
        "code_departement", "nom_departement",
        "code_region", "nom_region",
        "longitude", "latitude",
        "altitude_ref_alti",
        "code_masse_eau", "nom_masse_eau",
        "station_active",
        "finalite_station",
        "type_entite_hydro",
        "date_ouverture_station",
    )
)

save_gold(dim_stations, "dim_stations")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 dim_parametres

# COMMAND ----------

logger.info("=== DIM_PARAMETRES ===")

dim_parametres = (
    df_mesures
    .select(
        "code_parametre", "nom_parametre",
        "code_unite", "nom_unite", "symbole_unite",
        "code_support", "nom_support",
        "code_fraction_analysee", "nom_fraction_analysee"
    )
    .dropDuplicates(["code_parametre"])
    .withColumn("parametre_sk", F.monotonically_increasing_id())
    .select(
        "parametre_sk",
        "code_parametre", "nom_parametre",
        "code_unite", "nom_unite", "symbole_unite",
        "code_support", "nom_support",
        "code_fraction_analysee", "nom_fraction_analysee"
    )
)

save_gold(dim_parametres, "dim_parametres")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 dim_temps

# COMMAND ----------

logger.info("=== DIM_TEMPS ===")

dim_temps = (
    df_mesures
    .select("date_prelevement", "annee", "mois", "trimestre")
    .dropDuplicates(["date_prelevement"])
    .withColumn("temps_sk", F.date_format("date_prelevement", "yyyyMMdd").cast(IntegerType()))
    .withColumn("semaine", F.weekofyear("date_prelevement"))
    .withColumn("jour_semaine", F.dayofweek("date_prelevement"))
    .withColumn("nom_mois",
        F.when(F.col("mois") == 1, "Janvier")
         .when(F.col("mois") == 2, "Février")
         .when(F.col("mois") == 3, "Mars")
         .when(F.col("mois") == 4, "Avril")
         .when(F.col("mois") == 5, "Mai")
         .when(F.col("mois") == 6, "Juin")
         .when(F.col("mois") == 7, "Juillet")
         .when(F.col("mois") == 8, "Août")
         .when(F.col("mois") == 9, "Septembre")
         .when(F.col("mois") == 10, "Octobre")
         .when(F.col("mois") == 11, "Novembre")
         .otherwise("Décembre"))
    .withColumn("nom_trimestre",
        F.concat(F.lit("T"), F.col("trimestre").cast(StringType()),
                 F.lit(" "), F.col("annee").cast(StringType())))
    .withColumn("saison",
        F.when(F.col("mois").isin([12, 1, 2]), "Hiver")
         .when(F.col("mois").isin([3, 4, 5]), "Printemps")
         .when(F.col("mois").isin([6, 7, 8]), "Été")
         .otherwise("Automne"))
    .select(
        "temps_sk", "date_prelevement",
        "annee", "mois", "nom_mois",
        "trimestre", "nom_trimestre",
        "semaine", "jour_semaine", "saison"
    )
)

save_gold(dim_temps, "dim_temps")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tables de Faits

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 fact_mesures_qualite

# COMMAND ----------

logger.info("=== FACT_MESURES_QUALITE ===")

fact_mesures_qualite = (
    df_mesures
    # Seules les valeurs valides et sans outliers alimentent la table de faits analytique
    .filter(F.col("valeur_valide") & ~F.col("is_outlier"))
    # Jointure avec dim_stations pour récupérer la clé de substitution
    .join(
        dim_stations.select("station_sk", "code_station", "code_departement", "nom_departement"),
        "code_station", "left"
    )
    # Jointure avec dim_parametres
    .join(
        dim_parametres.select("parametre_sk", "code_parametre"),
        "code_parametre", "left"
    )
    # Jointure avec dim_temps
    .join(
        dim_temps.select("temps_sk", "date_prelevement"),
        "date_prelevement", "left"
    )
    .select(
        "code_analyse",
        "station_sk", "parametre_sk", "temps_sk",
        "code_station", "code_parametre",
        "code_departement", "nom_departement",
        "date_prelevement", "annee", "mois", "trimestre",
        "resultat", "resultat_corrige",
        "symbole_unite",
        "code_remarque", "libelle_remarque",
        "code_qualification", "libelle_qualification",
        "code_methode", "nom_methode"
    )
)

save_gold(fact_mesures_qualite, "fact_mesures_qualite")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 fact_conformite_normes

# COMMAND ----------

logger.info("=== FACT_CONFORMITE_NORMES ===")

fact_conformite_normes = (
    df_conformite
    .join(
        dim_stations.select("station_sk", "code_station", "code_departement", "nom_departement", "nom_region"),
        "code_station", "left"
    )
    .join(
        dim_temps.select("temps_sk", "date_prelevement"),
        "date_prelevement", "left"
    )
    .select(
        "code_analyse",
        "station_sk", "temps_sk",
        "code_station", "nom_station",
        "code_departement", "nom_departement", "nom_region",
        "code_parametre", "nom_parametre_ref",
        "date_prelevement", "annee", "mois", "trimestre",
        "resultat_corrige", "unite_ref",
        "seuil_alerte", "seuil_max",
        "statut_conformite"
    )
)

save_gold(fact_conformite_normes, "fact_conformite_normes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Tables Agrégées

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 agg_qualite_departement

# COMMAND ----------

logger.info("=== AGG_QUALITE_DEPARTEMENT ===")

agg_qualite_departement = (
    fact_mesures_qualite
    .groupBy("code_departement", "nom_departement", "code_parametre", "annee")
    .agg(
        F.count("code_analyse").alias("nb_mesures"),
        F.avg("resultat_corrige").alias("valeur_moyenne"),
        F.min("resultat_corrige").alias("valeur_min"),
        F.max("resultat_corrige").alias("valeur_max"),
        F.expr("percentile(resultat_corrige, 0.5)").alias("mediane"),
        F.expr("percentile(resultat_corrige, 0.9)").alias("percentile_90"),
        F.stddev("resultat_corrige").alias("ecart_type"),
        F.countDistinct("code_station").alias("nb_stations")
    )
    .join(
        dim_parametres.select("code_parametre", "nom_parametre", "symbole_unite"),
        "code_parametre", "left"
    )
)

save_gold(agg_qualite_departement, "agg_qualite_departement")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 agg_evolution_temporelle

# COMMAND ----------

logger.info("=== AGG_EVOLUTION_TEMPORELLE ===")

agg_evolution_temporelle = (
    fact_mesures_qualite
    .groupBy("code_parametre", "annee", "mois", "trimestre")
    .agg(
        F.count("code_analyse").alias("nb_mesures"),
        F.avg("resultat_corrige").alias("valeur_moyenne"),
        F.min("resultat_corrige").alias("valeur_min"),
        F.max("resultat_corrige").alias("valeur_max"),
        F.expr("percentile(resultat_corrige, 0.5)").alias("mediane"),
        F.countDistinct("code_station").alias("nb_stations"),
        F.countDistinct("code_departement").alias("nb_departements")
    )
    .join(
        dim_parametres.select("code_parametre", "nom_parametre", "symbole_unite"),
        "code_parametre", "left"
    )
    .orderBy("code_parametre", "annee", "mois")
)

save_gold(agg_evolution_temporelle, "agg_evolution_temporelle")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 agg_alertes_parametres

# COMMAND ----------

logger.info("=== AGG_ALERTES_PARAMETRES ===")

agg_alertes_parametres = (
    fact_conformite_normes
    .groupBy("code_departement", "nom_departement", "code_parametre", "nom_parametre_ref", "annee")
    .agg(
        F.count("code_analyse").alias("nb_mesures_total"),
        F.sum(F.when(F.col("statut_conformite") == "CONFORME", 1).otherwise(0)).alias("nb_conformes"),
        F.sum(F.when(F.col("statut_conformite") == "ALERTE", 1).otherwise(0)).alias("nb_alertes"),
        F.sum(F.when(F.col("statut_conformite") == "NON_CONFORME", 1).otherwise(0)).alias("nb_non_conformes"),
        F.avg("resultat_corrige").alias("valeur_moyenne"),
        F.max("resultat_corrige").alias("valeur_max_observee"),
        F.max("seuil_max").alias("seuil_max_reglementaire")
    )
    .withColumn("taux_conformite",
        F.round(F.col("nb_conformes") / F.col("nb_mesures_total") * 100, 2))
    .withColumn("taux_depassement",
        F.round(F.col("nb_non_conformes") / F.col("nb_mesures_total") * 100, 2))
    .orderBy("taux_depassement", ascending=False)
)

save_gold(agg_alertes_parametres, "agg_alertes_parametres")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Requêtes analytiques de validation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 10 départements avec le plus de dépassements

# COMMAND ----------

spark.sql("""
    SELECT
        nom_departement,
        nom_parametre_ref,
        annee,
        nb_mesures_total,
        nb_non_conformes,
        taux_depassement,
        valeur_max_observee,
        seuil_max_reglementaire
    FROM water_quality.agg_alertes_parametres
    WHERE nb_non_conformes > 0
    ORDER BY taux_depassement DESC
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Évolution annuelle des nitrates (paramètre 1340)

# COMMAND ----------

spark.sql("""
    SELECT
        annee,
        nom_parametre,
        ROUND(valeur_moyenne, 3) AS moyenne_mg_L,
        ROUND(valeur_max, 3) AS max_mg_L,
        ROUND(mediane, 3) AS mediane_mg_L,
        nb_mesures,
        nb_stations
    FROM water_quality.agg_evolution_temporelle
    WHERE code_parametre = '1340'
    ORDER BY annee, mois
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPIs globaux du pipeline

# COMMAND ----------

print("\n" + "="*60)
print("RESUME COUCHE GOLD")
print("="*60)

tables_gold = [
    "dim_stations", "dim_parametres", "dim_temps",
    "fact_mesures_qualite", "fact_conformite_normes",
    "agg_qualite_departement", "agg_evolution_temporelle", "agg_alertes_parametres"
]

for table in tables_gold:
    try:
        count = spark.table(f"{DATABASE_NAME}.{table}").count()
        print(f"  {table}: {count:,} enregistrements")
    except Exception as e:
        print(f"  {table}: ERREUR - {e}")

print("="*60)
print(f"Modélisation Gold terminée : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
