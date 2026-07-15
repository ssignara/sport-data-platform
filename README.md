# 🏃 Sport Data Platform

## 📖 Présentation

Sport Data Platform est une plateforme de traitement de données sportives développée dans le cadre du projet n°12 de la formation **Data Engineer OpenClassrooms**.

L'objectif est de mettre en place une architecture de données moderne permettant de :

- générer automatiquement des activités sportives d'entreprise ;
- diffuser ces activités sous forme d'événements via Redpanda (Kafka) ;
- charger les données dans PostgreSQL ;
- automatiser les traitements avec Kestra ;
- transformer les données SQL ;
- produire des indicateurs métier ;
- créer des tableaux de bord interactifs avec Metabase.

---

# 🏗️ Architecture

```
              Générateur Python
                     │
                     ▼
              Redpanda (Kafka)
                     │
                     ▼
              Consumer Python
                     │
                     ▼
              PostgreSQL (Bronze)
                     │
                     ▼
            Vues Analytics SQL
                     │
                     ▼
                Metabase
```

Les traitements sont orchestrés par **Kestra**.

---

# ⚙️ Technologies

| Technologie | Utilisation |
|-------------|-------------|
| Python | Génération et traitement des données |
| FastAPI | API REST |
| PostgreSQL | Base de données |
| Redpanda | Streaming d'événements |
| Kestra | Orchestration |
| SQL | Reporting |
| Metabase | Dashboard |
| Docker | Conteneurisation |

---

# 📂 Structure du projet

```text
sport-data-platform
│
├── data/
├── docs/
├── sql/
├── src/
│   ├── api/
│   ├── consumers/
│   ├── generators/
│   ├── loaders/
│   └── ...
│
├── tests/
├── docker-compose.yml
├── Dockerfile.app
├── Dockerfile.kestra
├── requirements.txt
└── README.md
```

---

# 🚀 Lancer le projet

## Cloner le dépôt

```bash
git clone https://github.com/ssignara/sport-data-platform.git
cd sport-data-platform
```

## Construire les conteneurs

```bash
docker compose up --build
```

Les services disponibles sont :

| Service | URL |
|----------|-----|
| API FastAPI | http://localhost:8001/docs |
| Kestra | http://localhost:8080 |
| Metabase | http://localhost:3000 |
| PostgreSQL | localhost:5432 |
| Redpanda | localhost:19092 |

---

# 📊 Fonctionnalités

## Génération d'activités

Le générateur crée automatiquement des activités sportives réalistes à partir des données RH.

Exemple :

- course à pied
- natation
- randonnée
- vélo
- fitness

---

## Streaming

Les activités sont publiées dans Redpanda puis consommées automatiquement avant d'être enregistrées dans PostgreSQL.

---

## Reporting

Le projet fournit plusieurs vues SQL :

- Global KPIs
- Activités par sport
- Activités par mois
- Synthèse par salarié

---

## Dashboard

Les indicateurs sont visualisables dans Metabase.

---

# 📦 Architecture de la base

```
bronze
│
├── employees
└── activities

analytics
│
├── global_kpis
├── employee_activity_summary
├── activities_by_sport
└── activities_by_month
```

---

# 🧪 Tests

Les traitements peuvent être exécutés via :

- API FastAPI
- Kestra
- SQL PostgreSQL

---

# 👤 Auteur

**Sokhna Signara Gueye**

Projet réalisé dans le cadre de la formation **Data Engineer OpenClassrooms**.