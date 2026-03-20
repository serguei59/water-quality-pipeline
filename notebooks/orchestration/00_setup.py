# Databricks notebook source
# MAGIC %md
# MAGIC # Setup - Initialisation de l'environnement
# MAGIC
# MAGIC Ce notebook initialise l'environnement Databricks avant le lancement du pipeline.
# MAGIC Il configure les bases de données, les chemins de stockage et vérifie la connectivité API.
# MAGIC
# MAGIC **A exécuter en premier, une seule fois.**

# COMMAND ----------

import requests
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Paramètres globaux

# COMMAND ----------

DATABASE_NAME = "water_quality"
BRONZE_PATH = "/mnt/delta/bronze"
SILVER_PATH = "/mnt/delta/silver"
GOLD_PATH   = "/mnt/delta/gold"

API_TEST_URL = "https://hubeau.eaufrance.fr/api/v1/qualite_rivieres/station_pc"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Création de la base de données

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME} COMMENT 'Pipeline qualité eau Hub\\'Eau - Architecture Médaillon'")
spark.sql(f"USE {DATABASE_NAME}")
logger.info(f"Base de données '{DATABASE_NAME}' prête.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Vérification de la connectivité API Hub'Eau

# COMMAND ----------

try:
    response = requests.get(API_TEST_URL, params={"size": 1}, timeout=15)
    response.raise_for_status()
    data = response.json()
    total = data.get("count", "?")
    logger.info(f"API Hub'Eau accessible. Nombre total de stations disponibles : {total}")
    print(f"API Hub'Eau OK - {total} stations disponibles")
except Exception as e:
    logger.error(f"Impossible de joindre l'API Hub'Eau : {e}")
    raise RuntimeError(f"Echec connexion API : {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Vérification de l'environnement Spark

# COMMAND ----------

print(f"Version Spark    : {spark.version}")
print(f"Version Python   : {spark.sparkContext.pythonVer}")
print(f"Nombre d'executors actifs : {spark.sparkContext.defaultParallelism}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Affichage du plan d'exécution

# COMMAND ----------

print("""
ORDRE D'EXECUTION DES NOTEBOOKS
================================
1. notebooks/orchestration/00_setup.py         <- Vous êtes ici
2. notebooks/bronze/01_ingestion_bronze.py
3. notebooks/silver/02_transformation_silver.py
4. notebooks/gold/03_modelisation_gold.py

TABLES PRODUITES
================================
Bronze :
  - bronze_stations
  - bronze_analyses
  - bronze_parametres

Silver :
  - silver_stations
  - silver_mesures
  - silver_conformite

Gold :
  - dim_stations
  - dim_parametres
  - dim_temps
  - fact_mesures_qualite
  - fact_conformite_normes
  - agg_qualite_departement
  - agg_evolution_temporelle
  - agg_alertes_parametres
""")

logger.info("Setup terminé. Vous pouvez lancer le notebook Bronze.")
