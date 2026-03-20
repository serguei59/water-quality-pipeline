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
2. Exécuter `orchestration/00_setup.py` pour initialiser les bases de données
3. Exécuter dans l'ordre : Bronze → Silver → Gold

## Endpoints API Utilisés

| Endpoint | Description |
|----------|-------------|
| `/qualite_rivieres/station_pc` | Stations de mesure |
| `/qualite_rivieres/analyse_pc` | Résultats d'analyses physico-chimiques |
| `/qualite_rivieres/operation_pc` | Opérations de mesure |
