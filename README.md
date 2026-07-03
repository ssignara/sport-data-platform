# 🏃 Sport Data Platform

## 📖 Présentation

Ce projet a été réalisé dans le cadre de la formation **Data Engineer** d'OpenClassrooms.

L'objectif est de concevoir une plateforme de traitement de données sportives permettant de :

- générer des activités sportives pour les collaborateurs d'une entreprise ;
- diffuser ces événements via une architecture événementielle ;
- stocker les données dans PostgreSQL ;
- automatiser les traitements avec Kestra ;
- contrôler la qualité des données avec Soda ;
- transformer les données avec dbt ;
- visualiser les indicateurs dans Metabase ;
- envoyer des notifications via Slack.

---

## 🏗️ Architecture technique

Le projet repose sur les composants suivants :

| Composant | Rôle |
|-----------|------|
| Python | Génération et traitement des données |
| PostgreSQL | Stockage des données |
| Redpanda | Broker de messages compatible Kafka |
| Kestra | Orchestration des traitements |
| dbt | Transformations SQL |
| Soda | Contrôle qualité |
| Metabase | Visualisation des données |
| Slack | Notifications |

---

## 📂 Structure du projet

```text
sport-data-platform/

├── config/
├── data/
├── dashboards/
├── docs/
├── kestra/
├── sql/
├── src/
├── tests/
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## 🚧 État d'avancement

### ✅ Terminé

- Docker
- PostgreSQL
- Redpanda
- Générateur d'activités
- Consumer Kafka
- Insertion PostgreSQL
- Notifications Slack (simulation)

### 🔄 En cours

- Kestra
- dbt
- Soda
- Metabase

---

## 👤 Auteur

**Sokhna Signara Gueye**