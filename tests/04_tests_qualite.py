# Databricks notebook source
# MAGIC %md
# MAGIC # Tests de Qualité des Données
# MAGIC
# MAGIC Ce notebook vérifie la qualité des données à chaque couche du pipeline.
# MAGIC Il doit être exécuté après les notebooks Bronze, Silver et Gold.
# MAGIC
# MAGIC **Principe :** chaque test renvoie PASS ou FAIL avec un message explicite.
# MAGIC Un résumé final indique le nombre de tests réussis / échoués.
# MAGIC Si des tests critiques échouent, une exception est levée pour bloquer le pipeline.

# COMMAND ----------

import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Infrastructure de tests

# COMMAND ----------

DATABASE_NAME = "water_quality"
spark.sql(f"USE {DATABASE_NAME}")

results = []  # liste de dict {nom, statut, message, critique}

def run_test(nom: str, condition: bool, message_fail: str, critique: bool = False):
    """
    Enregistre le résultat d'un test.

    Args:
        nom: Nom du test
        condition: True = PASS, False = FAIL
        message_fail: Message affiché en cas d'échec
        critique: Si True, un échec bloquera le pipeline
    """
    statut = "PASS" if condition else "FAIL"
    entry = {"nom": nom, "statut": statut, "message": message_fail if not condition else "", "critique": critique}
    results.append(entry)
    symbol = "✓" if condition else "✗"
    level = "CRITIQUE" if (not condition and critique) else ""
    logger.info(f"  [{statut}] {symbol} {nom} {level}")
    if not condition:
        logger.warning(f"       -> {message_fail}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tests Bronze — Existence et volumétrie

# COMMAND ----------

print("\n" + "="*60)
print("TESTS COUCHE BRONZE")
print("="*60)

TABLES_BRONZE = ["bronze_stations", "bronze_analyses", "bronze_parametres"]

for table in TABLES_BRONZE:
    try:
        df = spark.table(f"{DATABASE_NAME}.{table}")
        count = df.count()

        # Table non vide
        run_test(
            f"{table} — table non vide",
            count > 0,
            f"La table {table} est vide (0 enregistrement)",
            critique=True
        )

        # Colonnes _ingestion_timestamp et _source présentes (métadonnées Bronze)
        cols = df.columns
        run_test(
            f"{table} — métadonnées Bronze présentes",
            "_ingestion_timestamp" in cols and "_source" in cols,
            f"Colonnes de métadonnées manquantes dans {table}"
        )

    except Exception as e:
        run_test(f"{table} — accessible", False, f"Table inaccessible : {e}", critique=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tests Silver — Qualité et intégrité

# COMMAND ----------

print("\n" + "="*60)
print("TESTS COUCHE SILVER")
print("="*60)

# --- silver_stations ---
try:
    df_st = spark.table(f"{DATABASE_NAME}.silver_stations")

    run_test(
        "silver_stations — non vide",
        df_st.count() > 0,
        "silver_stations est vide",
        critique=True
    )

    # Pas de code_station NULL
    nulls = df_st.filter(F.col("code_station").isNull()).count()
    run_test(
        "silver_stations — code_station sans NULL",
        nulls == 0,
        f"{nulls} lignes avec code_station NULL",
        critique=True
    )

    # Pas de doublons sur code_station
    total = df_st.count()
    distincts = df_st.select("code_station").distinct().count()
    run_test(
        "silver_stations — pas de doublons",
        total == distincts,
        f"{total - distincts} doublons sur code_station"
    )

    # Coordonnées dans les bornes France métropolitaine
    hors_france = df_st.filter(
        ~F.col("latitude").between(41.0, 51.5) |
        ~F.col("longitude").between(-5.5, 10.0)
    ).count()
    run_test(
        "silver_stations — coordonnées en France métropolitaine",
        hors_france == 0,
        f"{hors_france} stations hors France métropolitaine"
    )

    # Latitude / longitude non NULL
    coords_nulles = df_st.filter(F.col("latitude").isNull() | F.col("longitude").isNull()).count()
    run_test(
        "silver_stations — coordonnées non nulles",
        coords_nulles == 0,
        f"{coords_nulles} stations sans coordonnées GPS"
    )

except Exception as e:
    run_test("silver_stations — accessible", False, str(e), critique=True)

# --- silver_mesures ---
try:
    df_m = spark.table(f"{DATABASE_NAME}.silver_mesures")
    total_m = df_m.count()

    run_test(
        "silver_mesures — non vide",
        total_m > 0,
        "silver_mesures est vide",
        critique=True
    )

    # Pas de doublons sur code_analyse
    distincts_m = df_m.select("code_analyse").distinct().count()
    run_test(
        "silver_mesures — pas de doublons (code_analyse)",
        total_m == distincts_m,
        f"{total_m - distincts_m} doublons sur code_analyse"
    )

    # Taux de valeurs valides >= 50%
    valides = df_m.filter(F.col("valeur_valide")).count()
    taux_valide = valides / total_m if total_m > 0 else 0
    run_test(
        "silver_mesures — taux valeurs valides >= 50%",
        taux_valide >= 0.5,
        f"Taux valeurs valides trop bas : {taux_valide*100:.1f}%"
    )

    # Taux d'outliers < 5%
    outliers = df_m.filter(F.col("is_outlier")).count()
    taux_outlier = outliers / total_m if total_m > 0 else 0
    run_test(
        "silver_mesures — taux outliers < 5%",
        taux_outlier < 0.05,
        f"Taux d'outliers anormalement élevé : {taux_outlier*100:.1f}%"
    )

    # Dates dans la fenêtre 2020-2024
    hors_periode = df_m.filter(
        (F.col("annee") < 2020) | (F.col("annee") > 2024)
    ).count()
    run_test(
        "silver_mesures — dates dans la fenêtre 2020-2024",
        hors_periode == 0,
        f"{hors_periode} mesures hors période 2020-2024"
    )

    # resultat_corrige toujours >= 0 sauf température
    negatifs = df_m.filter(
        (F.col("code_parametre") != "1301") &
        F.col("resultat_corrige").isNotNull() &
        (F.col("resultat_corrige") < 0)
    ).count()
    run_test(
        "silver_mesures — résultats corrigés >= 0 (hors température)",
        negatifs == 0,
        f"{negatifs} résultats négatifs inattendus"
    )

    # Colonne is_outlier présente
    run_test(
        "silver_mesures — colonne is_outlier présente",
        "is_outlier" in df_m.columns,
        "Colonne is_outlier absente de silver_mesures",
        critique=True
    )

except Exception as e:
    run_test("silver_mesures — accessible", False, str(e), critique=True)

# --- silver_conformite ---
try:
    df_c = spark.table(f"{DATABASE_NAME}.silver_conformite")

    run_test(
        "silver_conformite — non vide",
        df_c.count() > 0,
        "silver_conformite est vide",
        critique=True
    )

    # Statuts valides uniquement
    statuts_invalides = df_c.filter(
        ~F.col("statut_conformite").isin(["CONFORME", "ALERTE", "NON_CONFORME"])
    ).count()
    run_test(
        "silver_conformite — statuts valides",
        statuts_invalides == 0,
        f"{statuts_invalides} lignes avec statut inconnu"
    )

    # Pas de code_station NULL
    nulls_c = df_c.filter(F.col("code_station").isNull()).count()
    run_test(
        "silver_conformite — code_station sans NULL",
        nulls_c == 0,
        f"{nulls_c} lignes avec code_station NULL"
    )

except Exception as e:
    run_test("silver_conformite — accessible", False, str(e), critique=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tests Gold — Star schema et agrégats

# COMMAND ----------

print("\n" + "="*60)
print("TESTS COUCHE GOLD")
print("="*60)

# --- Dimensions ---
for dim, cle in [("dim_stations", "code_station"), ("dim_parametres", "code_parametre"), ("dim_temps", "temps_sk")]:
    try:
        df_dim = spark.table(f"{DATABASE_NAME}.{dim}")
        total_d = df_dim.count()

        run_test(f"{dim} — non vide", total_d > 0, f"{dim} est vide", critique=True)

        # Unicité de la clé naturelle
        distincts_d = df_dim.select(cle).distinct().count()
        run_test(
            f"{dim} — clé '{cle}' unique",
            total_d == distincts_d,
            f"{total_d - distincts_d} doublons sur {cle}"
        )

    except Exception as e:
        run_test(f"{dim} — accessible", False, str(e), critique=True)

# --- Tables de faits ---
for fact in ["fact_mesures_qualite", "fact_conformite_normes"]:
    try:
        df_fact = spark.table(f"{DATABASE_NAME}.{fact}")
        count_f = df_fact.count()

        run_test(f"{fact} — non vide", count_f > 0, f"{fact} est vide", critique=True)

        # Intégrité référentielle : station_sk non NULL
        orphelins = df_fact.filter(F.col("station_sk").isNull()).count()
        run_test(
            f"{fact} — station_sk non NULL",
            orphelins == 0,
            f"{orphelins} faits sans station_sk (rupture intégrité référentielle)"
        )

    except Exception as e:
        run_test(f"{fact} — accessible", False, str(e), critique=True)

# --- Agrégats ---
for agg in ["agg_qualite_departement", "agg_evolution_temporelle", "agg_alertes_parametres"]:
    try:
        count_a = spark.table(f"{DATABASE_NAME}.{agg}").count()
        run_test(f"{agg} — non vide", count_a > 0, f"{agg} est vide")
    except Exception as e:
        run_test(f"{agg} — accessible", False, str(e))

# Cohérence : le nombre de faits Gold doit être <= Silver mesures valides
try:
    nb_fact = spark.table(f"{DATABASE_NAME}.fact_mesures_qualite").count()
    nb_silver_valides = spark.table(f"{DATABASE_NAME}.silver_mesures") \
        .filter(F.col("valeur_valide") & ~F.col("is_outlier")).count()
    run_test(
        "fact_mesures_qualite — volumétrie cohérente avec silver_mesures",
        nb_fact <= nb_silver_valides,
        f"Gold ({nb_fact:,}) > Silver valides ({nb_silver_valides:,}) : incohérence de volumétrie",
        critique=True
    )
except Exception as e:
    run_test("cohérence Gold/Silver", False, str(e), critique=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Résumé des tests

# COMMAND ----------

nb_pass = sum(1 for r in results if r["statut"] == "PASS")
nb_fail = sum(1 for r in results if r["statut"] == "FAIL")
nb_critique = sum(1 for r in results if r["statut"] == "FAIL" and r["critique"])
nb_total = len(results)

print("\n" + "="*60)
print("RESUME DES TESTS QUALITE")
print("="*60)
print(f"  Total  : {nb_total} tests")
print(f"  PASS   : {nb_pass}")
print(f"  FAIL   : {nb_fail}")
if nb_critique > 0:
    print(f"  CRITIQUE (bloquants) : {nb_critique}")
print("="*60)

if nb_fail > 0:
    print("\nTests en échec :")
    for r in results:
        if r["statut"] == "FAIL":
            tag = " [CRITIQUE]" if r["critique"] else ""
            print(f"  ✗ {r['nom']}{tag}")
            print(f"    -> {r['message']}")

print(f"\nTests terminés : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Bloque le pipeline si des tests critiques échouent
if nb_critique > 0:
    raise AssertionError(
        f"{nb_critique} test(s) critique(s) en échec. "
        "Corriger les problèmes avant de poursuivre le pipeline."
    )
