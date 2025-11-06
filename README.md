# 🎮 League of Legends Data Pipeline

Pipeline ETL pour analyser les données de matchs League of Legends.

## 🏗️ Architecture

- **Extraction** : Airflow + Riot Games API
- **Stockage** : PostgreSQL
- **Transformation** : dbt
- **Visualisation** : Streamlit
- **Infrastructure** : Docker Compose

## 🚀 Quick Start
```bash
# 1. Clone le repo
git clone https://github.com/StevenSivakaran/league-of-legends-data-pipeline.git
cd league-of-legends-data-pipeline

# 2. Configure les variables d'environnement
cp .env.example .env
# Édite .env et ajoute ta clé API Riot

# 3. Lance l'infrastructure
docker-compose up -d

# 4. Accède aux interfaces
# Airflow : http://localhost:8080 (admin/admin)
# Streamlit : http://localhost:8501
```

## 📊 Données collectées

- Détails des matchs (durée, mode, version)
- Statistiques des joueurs (KDA, CS, vision score, dégâts)
- Métriques par champion (winrate, pick rate, ban rate)

## 🎯 Objectifs du projet

Projet portfolio pour démontrer :
- ✅ Extraction de données via API
- ✅ Orchestration avec Airflow
- ✅ Modélisation de données avec dbt
- ✅ Containerisation avec Docker
- ✅ Visualisation interactive

## 👤 Auteur

**Steven** - Data Engineer
- GitHub : [@StevenSivakaran] (https://github.com/StevenSivakaran)
- LinkedIn : [StevenSIVAKARAN] (https://www.linkedin.com/in/steven-sivakaran-data-engineer/)

## 📄 Licence

MIT
