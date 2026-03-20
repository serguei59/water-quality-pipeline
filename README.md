# Water Quality Pipeline - Hub'Eau

Pipeline de data engineering pour analyser les données de qualité de l'eau en France.

## Architecture Médaillon

```
Bronze (Données Brutes) → Silver (Données Nettoyées) → Gold (Données Analytiques)
```

| Couche | Description | Tables |
|--------|-------------|--------|
| **Bronze** | Données brutes ingérées depuis l'API Hub'Eau | `bronze_stations`, `bronze_analyses`, `bronze_parametres` |
| **Silver** | Données nettoyées, dédupliquées, typées | `silver_stations`, `silver_mesures`, `silver_conformite` |
| **Gold** | Tables analytiques pour dashboards | `dim_*`, `fact_*`, `agg_*` |

## Source de Données

- **API** : Hub'Eau - Qualité des cours d'eau
- **URL** : https://hubeau.eaufrance.fr/api/v1/qualite_rivieres/
- **Volume** : Plusieurs millions de mesures (2020-2024)
- **Paramètres** : pH, température, nitrates, phosphates, métaux lourds, pesticides

## Structure du Projet

```
water-quality-pipeline/
├── notebooks/
│   ├── bronze/          # Ingestion API Hub'Eau
│   ├── silver/          # Nettoyage et transformation
│   ├── gold/            # Modélisation analytique
│   └── orchestration/   # Setup et workflows
├── config/              # Paramètres de configuration
├── docs/                # Documentation
├── tests/               # Tests de qualité des données
└── data/samples/        # Échantillons de données
```

## Technologies

- **Databricks** (Runtime 13.3 LTS+)
- **PySpark** - Traitement distribué
- **Delta Lake** - Format de stockage
- **SQL** - Requêtes analytiques
- **Python** - Scripts d'ingestion

## Prérequis

- Workspace Databricks (Azure/AWS/Community Edition)
- Cluster avec Runtime 13.3 LTS ou supérieur
- Accès internet pour l'API Hub'Eau (aucune clé API requise)

## Lancement

1. Importer les notebooks dans Databricks
2. Exécuter `notebooks/orchestration/00_setup.py` pour initialiser la base de données et vérifier la connectivité API
3. Exécuter dans l'ordre :
   - `notebooks/bronze/01_ingestion_bronze.py`
   - `notebooks/silver/02_transformation_silver.py`
   - `notebooks/gold/03_modelisation_gold.py`
4. Exécuter `tests/04_tests_qualite.py` pour valider la qualité des données

## Tests de Qualité

Le notebook `tests/04_tests_qualite.py` vérifie automatiquement :

| Couche | Contrôles |
|--------|-----------|
| Bronze | Tables non vides, métadonnées d'ingestion présentes |
| Silver | Absence de doublons, taux de valeurs valides ≥ 50%, taux d'outliers < 5%, dates dans la fenêtre 2020-2024 |
| Gold | Unicité des clés de dimension, intégrité référentielle, cohérence de volumétrie Gold/Silver |

Les tests critiques lèvent une exception pour bloquer le pipeline en cas d'anomalie.

## Qualité des Données

- **Valeurs sous seuil de détection** : remplacées par LD/2 ou LQ/2 (convention métrologique standard)
- **Outliers** : détectés par paramètre via des plages physiquement plausibles (référentiels SANDRE/OMS), flagués `is_outlier` et exclus des analyses Gold
- **Conformité réglementaire** : évaluée selon la Directive Cadre sur l'Eau 2000/60/CE et la directive eau potable 98/83/CE

## Endpoints API Utilisés

| Endpoint | Description |
|----------|-------------|
| `/qualite_rivieres/station_pc` | Stations de mesure |
| `/qualite_rivieres/analyse_pc` | Résultats d'analyses physico-chimiques |
| `/qualite_rivieres/operation_pc` | Opérations de mesure |
