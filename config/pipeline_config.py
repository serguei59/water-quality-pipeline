"""
Configuration centrale du pipeline water-quality.
Importer ce module dans les notebooks Databricks.
"""

# API Hub'Eau
API_BASE_URL = "https://hubeau.eaufrance.fr/api/v1/qualite_rivieres"
API_TIMEOUT = 30
API_PAGE_SIZE = 1000

# Fenêtre temporelle d'ingestion
DATE_DEBUT = "2020-01-01"
DATE_FIN = "2024-12-31"

# Départements ciblés (10 grandes métropoles françaises)
DEPARTEMENTS = ["75", "69", "13", "33", "59", "31", "06", "67", "44", "76"]

# Chemins Delta Lake
DATABASE_NAME = "water_quality"
BRONZE_PATH = "/mnt/delta/bronze"
SILVER_PATH = "/mnt/delta/silver"
GOLD_PATH = "/mnt/delta/gold"

# Seuils de conformité (Directive 2000/60/CE et directive eau potable 98/83/CE)
SEUILS_CONFORMITE = {
    "1340": {"nom": "Nitrates",          "seuil_alerte": 25.0,  "seuil_max": 50.0,  "unite": "mg/L",       "sens": "max"},
    "1350": {"nom": "Nitrites",          "seuil_alerte": 0.05,  "seuil_max": 0.1,   "unite": "mg/L",       "sens": "max"},
    "1433": {"nom": "Phosphore total",   "seuil_alerte": 0.1,   "seuil_max": 0.2,   "unite": "mg/L",       "sens": "max"},
    "1302": {"nom": "pH",                "seuil_alerte": None,  "seuil_max": None,  "unite": "unité pH",   "plage_min": 6.5, "plage_max": 9.0},
    "1301": {"nom": "Température",       "seuil_alerte": 20.0,  "seuil_max": 25.0,  "unite": "°C",         "sens": "max"},
    "1311": {"nom": "Oxygène dissous",   "seuil_alerte": 5.0,   "seuil_max": None,  "unite": "mg/L",       "sens": "min"},
    "1841": {"nom": "Escherichia coli",  "seuil_alerte": 1000.0,"seuil_max": 2000.0,"unite": "UFC/100mL",  "sens": "max"},
}

# Codes de remarques Hub'Eau
CODES_REMARQUES = {
    "1":  "Valeur normale",
    "2":  "Valeur inférieure au seuil de quantification",
    "3":  "Valeur inférieure au seuil de détection",
    "4":  "Valeur supérieure au seuil de saturation",
    "5":  "Valeur douteuse",
    "6":  "Valeur non réalisée",
    "7":  "Valeur non significative",
    "10": "Valeur réestimée",
}

# Codes remarques considérés comme valeurs valides
CODES_VALIDES = ["1", "10"]
