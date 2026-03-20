# Databricks notebook source
# MAGIC %md
# MAGIC # Couche Bronze - Ingestion des données Hub'Eau
# MAGIC
# MAGIC Ce notebook ingère les données brutes depuis l'API Hub'Eau (qualité des cours d'eau)
# MAGIC et les stocke au format Delta Lake dans la couche Bronze.
# MAGIC
# MAGIC **Tables créées :**
# MAGIC - `bronze_stations` : Métadonnées des stations de mesure
# MAGIC - `bronze_analyses` : Résultats d'analyses physico-chimiques
# MAGIC - `bronze_parametres` : Référentiel des paramètres mesurés

# COMMAND ----------

import requests
import json
import logging
from datetime import datetime
from urllib.parse import urlparse, parse_qs
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

API_BASE_URL = "https://hubeau.eaufrance.fr/api/v1/qualite_rivieres"

CONFIG = {
    "date_debut": "2020-01-01",
    "date_fin": "2024-12-31",
    "departements": ["75", "69", "13", "33", "59", "31", "06", "67", "44", "76"],
    "page_size": 1000,
    "timeout": 30,
}

DATABASE_NAME = "water_quality"
BRONZE_PATH = "/mnt/delta/bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Initialisation de la base de données

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
spark.sql(f"USE {DATABASE_NAME}")
logger.info(f"Base de données '{DATABASE_NAME}' initialisée.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Fonctions utilitaires

# COMMAND ----------

def fetch_api(endpoint: str, params: dict) -> list:
    """
    Appelle l'API Hub'Eau avec pagination automatique via curseur.

    Args:
        endpoint: Chemin de l'endpoint (ex: /station_pc)
        params: Paramètres de la requête

    Returns:
        Liste de tous les enregistrements récupérés
    """
    url = f"{API_BASE_URL}{endpoint}"
    all_records = []
    params["size"] = CONFIG["page_size"]
    page = 1

    while True:
        try:
            logger.info(f"Appel API {endpoint} - page {page}")
            response = requests.get(url, params=params, timeout=CONFIG["timeout"])
            response.raise_for_status()
            data = response.json()

            records = data.get("data", [])
            all_records.extend(records)
            logger.info(f"  -> {len(records)} enregistrements (total: {len(all_records)})")

            next_url = data.get("next", None)
            if not next_url or len(records) == 0:
                break

            parsed = urlparse(next_url)
            cursor = parse_qs(parsed.query).get("cursor", [None])[0]
            if cursor:
                params["cursor"] = cursor
            else:
                break

            page += 1

        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur API {endpoint}: {e}")
            break

    return all_records


def save_bronze(records: list, table_name: str):
    """
    Sauvegarde les données brutes au format Delta Lake.

    Args:
        records: Liste de dictionnaires (données brutes API)
        table_name: Nom de la table Delta cible
    """
    if not records:
        logger.warning(f"Aucune donnée à sauvegarder pour {table_name}")
        return 0

    df = spark.createDataFrame(records)
    df = (df
          .withColumn("_ingestion_timestamp", current_timestamp())
          .withColumn("_source", lit("hubeau_api"))
          .withColumn("_batch_date", lit(datetime.now().strftime("%Y-%m-%d"))))

    path = f"{BRONZE_PATH}/{table_name}"
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .option("path", path)
       .saveAsTable(f"{DATABASE_NAME}.{table_name}"))

    count = df.count()
    logger.info(f"Table '{table_name}' sauvegardée : {count:,} enregistrements")
    return count

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ingestion des Stations de Mesure (bronze_stations)

# COMMAND ----------

logger.info("=== INGESTION STATIONS ===")

stations_records = []
for dept in CONFIG["departements"]:
    params = {
        "code_departement": dept,
        "fields": (
            "code_station,nom_station,uri_station,"
            "code_commune,nom_commune,code_departement,nom_departement,"
            "code_region,nom_region,"
            "coordonnee_x,coordonnee_y,longitude,latitude,"
            "altitude_ref_alti,code_masse_eau,nom_masse_eau,"
            "date_ouverture_station,date_fermeture_station,"
            "finalite_station,type_entite_hydro"
        ),
    }
    records = fetch_api("/station_pc", params)
    stations_records.extend(records)
    logger.info(f"Département {dept}: {len(records)} stations")

# Dédoublonnage par code_station
seen = set()
unique_stations = []
for r in stations_records:
    key = r.get("code_station")
    if key and key not in seen:
        seen.add(key)
        unique_stations.append(r)

logger.info(f"Total stations uniques : {len(unique_stations)}")
save_bronze(unique_stations, "bronze_stations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Ingestion des Analyses Physico-chimiques (bronze_analyses)

# COMMAND ----------

logger.info("=== INGESTION ANALYSES ===")

analyses_records = []
for dept in CONFIG["departements"]:
    params = {
        "code_departement": dept,
        "date_debut_prelevement": CONFIG["date_debut"],
        "date_fin_prelevement": CONFIG["date_fin"],
        "fields": (
            "code_analyse,code_station,nom_station,"
            "code_parametre,nom_parametre,code_unite,nom_unite,symbole_unite,"
            "date_prelevement,heure_prelevement,"
            "resultat,code_remarque,nom_remarque,"
            "limite_detection,limite_quantification,"
            "code_qualification,libelle_qualification,"
            "code_support,nom_support,"
            "code_fraction_analysee,nom_fraction_analysee,"
            "code_methode,nom_methode"
        ),
    }
    records = fetch_api("/analyse_pc", params)
    analyses_records.extend(records)
    logger.info(f"Département {dept}: {len(records)} analyses")

logger.info(f"Total analyses : {len(analyses_records)}")
save_bronze(analyses_records, "bronze_analyses")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Ingestion des Opérations de Mesure (bronze_parametres)

# COMMAND ----------

logger.info("=== INGESTION OPERATIONS ===")

operations_records = []
for dept in CONFIG["departements"]:
    params = {
        "code_departement": dept,
        "date_debut_prelevement": CONFIG["date_debut"],
        "date_fin_prelevement": CONFIG["date_fin"],
        "fields": (
            "code_operation,code_station,nom_station,"
            "date_prelevement,heure_prelevement,"
            "code_prelevement,code_reseau,nom_reseau,"
            "code_producteur,nom_producteur,"
            "code_laboratoire,nom_laboratoire,"
            "code_support,nom_support"
        ),
    }
    records = fetch_api("/operation_pc", params)
    operations_records.extend(records)
    logger.info(f"Département {dept}: {len(records)} opérations")

logger.info(f"Total opérations : {len(operations_records)}")
save_bronze(operations_records, "bronze_parametres")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validation et résumé

# COMMAND ----------

print("\n" + "="*60)
print("RESUME INGESTION BRONZE")
print("="*60)

for table in ["bronze_stations", "bronze_analyses", "bronze_parametres"]:
    try:
        count = spark.table(f"{DATABASE_NAME}.{table}").count()
        print(f"  {table}: {count:,} enregistrements")
    except Exception as e:
        print(f"  {table}: ERREUR - {e}")

print("="*60)
print(f"Ingestion terminée : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Aperçu des données

# COMMAND ----------

print("--- Aperçu bronze_stations ---")
spark.table(f"{DATABASE_NAME}.bronze_stations").show(5, truncate=False)

print("--- Aperçu bronze_analyses ---")
spark.table(f"{DATABASE_NAME}.bronze_analyses").show(5, truncate=False)

print("--- Aperçu bronze_parametres ---")
spark.table(f"{DATABASE_NAME}.bronze_parametres").show(5, truncate=False)
