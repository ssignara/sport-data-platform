# 🏃 Sport Data Platform

## 📖 Présentation

Sport Data Platform est une plateforme de traitement de données sportives développée dans le cadre du projet n°12 de la formation **Data Engineer OpenClassrooms**.

L'objectif est de mettre en place une architecture de données moderne capable d'automatiser l'ensemble du cycle de vie des données sportives d'une entreprise, depuis la génération des activités jusqu'à leur visualisation dans des tableaux de bord décisionnels.

Le projet repose sur une architecture orientée événements utilisant **Redpanda (Kafka)**, **PostgreSQL**, **FastAPI**, **Kestra**, **Soda** et **Metabase**.

---

# 🎯 Objectifs

La plateforme permet de :

- générer automatiquement des activités sportives réalistes à partir des données RH ;
- diffuser les activités sous forme d'événements Kafka ;
- charger les données dans PostgreSQL ;
- transformer les données afin de produire des indicateurs métier ;
- mettre les données transformées à disposition du reporting via le schéma Analytics ;
- automatiser les traitements grâce à Kestra ;
- contrôler la qualité des données avec Soda ;
- alimenter des tableaux de bord interactifs dans Metabase.

---

# 🏗️ Architecture de la solution

```
                Données RH (Excel)
                        │
                        ▼
            Chargement PostgreSQL (Employees)
                        │
                        ▼
            Génération des activités (Python)
                        │
                        ▼
                 API FastAPI
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
          Transformations SQL
                        │
                        ▼
            Schéma Analytics
                 │             │
                 ▼             ▼
             Soda         Metabase
```

L'ensemble des traitements est orchestré par **Kestra**.

Cette architecture met en œuvre un pipeline **ETL de bout en bout**, depuis l'extraction des données RH et sportives jusqu'à leur transformation, leur contrôle qualité et leur restitution dans Metabase.

---

# 🛠️ Technologies utilisées

| Technologie | Rôle |
|-------------|------|
| Python | Génération et traitement des données |
| FastAPI | Exposition des traitements via API REST |
| Redpanda | Diffusion des événements (Kafka) |
| PostgreSQL | Stockage des données |
| SQL | Transformations et reporting |
| Kestra | Orchestration des workflows |
| Soda | Contrôle qualité des données |
| Metabase | Visualisation des indicateurs |
| Docker | Conteneurisation de la plateforme |

---

# 🔄 Pipeline ETL de bout en bout

Le pipeline couvre l'ensemble du cycle **ETL (Extraction → Transformation → Chargement)** en combinant traitement événementiel et transformations SQL.

## 1. Extraction

Les données exploitées par la plateforme proviennent de deux sources :

- les données RH chargées depuis un fichier Excel ;
- les activités sportives générées automatiquement par les scripts Python.

Les activités sont ensuite publiées dans **Redpanda** sous forme d'événements Kafka.

---

## 2. Transformation

Les événements sont consommés par un consumer Python puis enregistrés dans les tables **Bronze** de PostgreSQL.

Les transformations métier sont ensuite réalisées par le script SQL :

`sql/03_create_reporting_views.sql`

Ces traitements permettent notamment de :

- préparer et normaliser les activités ;
- appliquer les règles métier ;
- calculer les indicateurs par salarié ;
- agréger les données par sport, par mois et par business unit ;
- produire les indicateurs globaux utilisés pour le reporting.

Les transformations sont exécutées via l'endpoint FastAPI `/transform`, orchestré par Kestra.

---

## 3. Chargement

Les données transformées sont mises à disposition dans le schéma **Analytics** de PostgreSQL à travers plusieurs vues SQL :

- global_kpis
- employee_activity_summary
- activities_by_sport
- activities_by_month
- business_unit_summary

Ces vues sont directement exploitées par **Metabase** pour alimenter les tableaux de bord.

---

## 4. Contrôle qualité

Avant leur exploitation, les données sont contrôlées grâce à **Soda**.

Les vérifications portent notamment sur :

- la présence des données obligatoires ;
- la cohérence des enregistrements ;
- la qualité des tables utilisées pour le reporting.

---

## 5. Restitution

Les tableaux de bord Metabase exploitent directement les vues du schéma Analytics.

---

Le pipeline suit donc le flux suivant :

```text
Fichiers RH + Génération Python
            │
            ▼
     Publication Redpanda
            │
            ▼
     Consumer Python
            │
            ▼
 PostgreSQL (Bronze)
            │
            ▼
 Transformations SQL
            │
            ▼
 Schéma Analytics
            │
            ▼
      Contrôles Soda
            │
            ▼
        Metabase
```

---

# ⚙️ Orchestration Kestra

Le projet distingue **deux workflows complémentaires**.

## Workflow d'initialisation

Ce workflow est exécuté **une seule fois** lors de l'initialisation de la plateforme.

Il permet de :

1. charger les données RH ;
2. générer l'historique des activités sportives ;
3. publier les événements dans Redpanda ;
4. consommer les événements ;
5. alimenter les tables Bronze ;
6. créer les vues Analytics.

Cette étape initialise complètement la plateforme.

---

## Workflow quotidien

Une fois la plateforme initialisée, Kestra déclenche automatiquement un workflow quotidien grâce à un **trigger Cron**.

Ce workflow ne traite que les nouvelles données générées.

Les étapes exécutées sont :

1. génération des activités du jour ;
2. publication dans Redpanda ;
3. consommation des événements ;
4. insertion dans PostgreSQL ;
5. transformations SQL ;
6. contrôles Soda ;
7. mise à jour des tableaux de bord Metabase.

Cette approche évite de régénérer l'historique complet à chaque exécution et garantit un traitement incrémental des données.

---

## Modularité et gestion des erreurs

Le pipeline est organisé en étapes indépendantes afin de faciliter sa maintenance, son évolution et la reprise des traitements en cas d'échec.

Les principales étapes sont séparées par responsabilité :

1. génération des activités ;
2. publication des événements dans Redpanda ;
3. consommation des événements ;
4. chargement dans le schéma Bronze ;
5. transformation des données ;
6. mise à disposition dans le schéma Analytics ;
7. contrôle qualité avec Soda.

Cette organisation modulaire permet d'identifier précisément l'étape en erreur sans avoir à modifier l'ensemble du pipeline.

Les workflows Kestra assurent le suivi de l'état de chaque tâche. En cas d'échec, l'exécution est marquée comme échouée et les journaux permettent d'identifier la cause de l'incident.

La fiabilité du pipeline repose également sur :

- la séparation entre le workflow d'initialisation et le workflow quotidien ;
- le traitement incrémental des nouvelles activités ;
- les contrôles de qualité exécutés après les transformations ;
- la centralisation des journaux d'exécution dans Kestra ;
- la possibilité de relancer le workflow ou uniquement les tâches en échec ;
- l'utilisation d'identifiants uniques afin de limiter les doublons lors d'une reprise ;
- l'arrêt du pipeline lorsqu'une étape critique échoue, afin d'éviter d'alimenter le reporting avec des données incorrectes.

Cette architecture facilite ainsi la reprise après interruption et empêche la publication de données non contrôlées dans les tableaux de bord.

---

# 🌐 API FastAPI

La plateforme expose plusieurs endpoints REST.

| Endpoint | Description |
|----------|-------------|
| `/generate` | Génération de l'historique des activités |
| `/generate-daily` | Génération quotidienne des nouvelles activités |
| `/transform` | Exécution des transformations SQL |

---

# 🗄️ Architecture de la base de données

## Schéma Bronze

- employees
- activities

## Schéma Analytics

- global_kpis
- employee_activity_summary
- activities_by_sport
- activities_by_month
- business_unit_summary

---

# 🔧 Configuration

Les paramètres de la plateforme sont externalisés afin de faciliter son déploiement.

La configuration repose sur :

- les variables d'environnement ;
- le fichier `.env` (non versionné) ;
- le fichier `.env.example` (modèle de configuration) ;
- `docker-compose.yml` ;
- `config/settings.py`.

Les principaux paramètres concernent :

- PostgreSQL ;
- Redpanda ;
- FastAPI ;
- Kestra ;
- Slack.

---

# 🚀 Installation

## 1. Cloner le dépôt

```bash
git clone https://github.com/ssignara/sport-data-platform.git
cd sport-data-platform
```

## 2. Configurer l'environnement

Créer un fichier `.env` à partir du modèle `.env.example`, puis renseigner les variables nécessaires.

## 3. Démarrer les conteneurs

```bash
docker compose up --build
```

Les principaux services sont accessibles via :

| Service | URL |
|----------|-----|
| FastAPI | http://localhost:8001/docs |
| Kestra | http://localhost:8080 |
| Metabase | http://localhost:3000 |

---

# 📂 Structure du projet

```text
sport-data-platform
│
├── config/
├── data/
├── docs/
├── kestra/
├── soda/
├── sql/
├── src/
│   ├── api/
│   ├── consumers/
│   ├── generators/
│   ├── loaders/
│   ├── notifications/
│   └── producers/
│
├── tests/
├── docker-compose.yml
├── requirements.txt
├── README.md
└── .env.example
```

---

# 📊 Visualisation

Les tableaux de bord Metabase permettent notamment de suivre :

- les indicateurs globaux ;
- les activités par sport ;
- les activités par mois ;
- la participation des collaborateurs ;
- les statistiques par salarié.

---

# 🧪 Contrôle qualité

Les règles Soda garantissent la qualité des données utilisées pour le reporting avant leur exploitation dans Metabase.

---

# 🚀 Perspectives d'évolution

L'architecture a été conçue de manière modulaire afin de pouvoir évoluer vers des volumes de données plus importants et répondre à de nouveaux besoins métier.

Parmi les principales pistes d'amélioration :

### 📈 Scalabilité

- Déploiement de plusieurs consommateurs Kafka afin de paralléliser le traitement des événements.
- Partitionnement des topics Redpanda pour améliorer le débit.
- Optimisation des index PostgreSQL et partitionnement des tables d'activités.

### 📥 Intégration de nouvelles sources

Le pipeline pourrait être enrichi avec d'autres sources de données telles que :

- applications de suivi sportif (Strava, Garmin, Fitbit...) ;
- fichiers CSV ou API provenant d'autres services RH ;
- données issues d'objets connectés.

Grâce à l'architecture événementielle, ces nouvelles sources pourraient publier leurs événements dans Redpanda sans modifier le reste du pipeline.

### 📊 Nouveaux KPIs (évolution métier)

Le schéma Analytics pourrait être enrichi avec de nouveaux KPIs, par exemple :

- taux de participation par service ;
- évolution mensuelle de l'activité physique ;
- classement des collaborateurs les plus actifs ;
- suivi des objectifs sportifs individuels ;
- estimation des émissions de CO₂ évitées grâce aux mobilités douces.

### ☁️ Industrialisation

Pour une utilisation en production, plusieurs évolutions pourraient être envisagées :

- déploiement sur une plateforme cloud ;
- orchestration distribuée des traitements ;
- supervision centralisée des pipelines ;
- intégration d'une chaîne CI/CD pour automatiser les tests et les déploiements.

---

# 👤 Auteur

**Sokhna Signara Gueye**

Projet réalisé dans le cadre de la formation **Data Engineer – OpenClassrooms**.