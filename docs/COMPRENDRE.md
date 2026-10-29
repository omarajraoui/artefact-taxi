# Comprendre le projet : le problème, la solution, et ce qu'il faut savoir

Ce document est fait pour quelqu'un qui découvre le projet (un relecteur, un recruteur, un collègue, ou toi-même
avant un entretien). Il répond, dans l'ordre, à cinq questions :

1. **Quel problème nous a-t-on donné ?**
2. **Qu'est-ce qu'un pipeline de données, un datalake, une architecture médaillon ?**
3. **Quelle solution avons-nous construite, et comment répond-elle à chaque exigence ?**
4. **Pourquoi Docker (et pourquoi chaque autre outil) ?**
5. **Que faut-il savoir pour la défendre, et où apprendre ?**

---

## 1. Le problème qu'on nous a donné

**Le contexte.** Artefact construit des plateformes de données pour ses clients. Le test nous place dans l'équipe
data d'une entreprise de **mobilité urbaine** (taxis, VTC…). L'équipe **BI** (*Business Intelligence* : ceux qui font
les tableaux de bord et les analyses pour la direction) a besoin de chiffres fiables :

* combien gagne-t-on **par jour** ?
* combien de temps durent les trajets ?
* **où** les clients montent-ils le plus ?

**Le problème.** Les données brutes ne sont pas utilisables telles quelles :

* elles arrivent dans des **fichiers sources** (CSV) téléchargés depuis une API, en gros volume ;
* elles sont **sales** : dates illisibles, trajets de 0 seconde, montants négatifs, doublons ;
* elles sont **trop détaillées** : un analyste ne veut pas 1,4 million de lignes, il veut « le revenu de mardi » ;
* il faut pouvoir **refaire** le calcul demain, sur une autre machine, sans tout reconstruire à la main.

**Ce qu'on nous demande précisément** (exigences du sujet) :

| # | Exigence | En clair |
|---|---|---|
| 1 | Ingérer les données brutes dans un datalake | récupérer les trajets et les ranger, sans les modifier |
| 2 | Nettoyer et transformer en tables analytiques | des données propres, typées, sans doublons, contrôlées |
| 3 | Exposer des agrégats prêts pour la BI | revenus par jour, durée moyenne, top zones… |
| 4 | Architecture en couches (raw → clean → aggregated) | bronze, silver, gold, chaque couche stockée |
| 5 | Stockage objet comme socle | un « S3 » local |
| 6 | Orchestration (Airflow ou Dagster) | les étapes enchaînées automatiquement, avec relances et suivi |
| 7 | Traitement avec Spark (PySpark) | le moteur de calcul imposé |
| 8 | Docker Compose, une seule commande | `docker compose up` doit tout faire tourner |
| 9 | Reproductible, volume raisonnable | une période fixée, documentée, retéléchargeable |
| 10 | Livrables : dépôt Git, README (lancement, schéma, choix, suite) | le projet partageable et expliqué |

---

## 2. Les notions de base (à comprendre avant tout)

### Un pipeline de données
Une **chaîne d'étapes automatiques** qui part de données brutes et produit des données utiles, comme une usine :
*réception* (ingestion) → *contrôle et nettoyage* (transformation) → *produit fini* (agrégats). On l'appelle aussi
**ETL** (*Extract, Transform, Load*) ou **ELT** quand on charge d'abord le brut puis on transforme, comme ici.

### Un datalake (lac de données)
Un **grand espace de stockage de fichiers**, peu cher, où l'on garde les données **brutes et transformées**, dans
des formats ouverts (CSV, Parquet). Différence avec un **data warehouse** (entrepôt) : l'entrepôt est une base SQL
structurée et plus chère ; le lac garde tout, y compris le brut, et plusieurs outils peuvent le lire.

### Le stockage objet (S3)
La technologie qui sert de socle aux datalakes dans le cloud : **Amazon S3**, Google Cloud Storage, Azure Blob. On y
range des **objets** (des fichiers) dans des **buckets**, adressés par une **clé** qui ressemble à un chemin
(`bronze/taxi_trips/month=2023-01/page=00001.csv`). Le « protocole S3 » est devenu un standard : beaucoup d'outils
savent le parler, et des logiciels comme SeaweedFS l'imitent en local.

### L'architecture médaillon (bronze / silver / gold)
Une façon d'organiser le lac en **trois couches de qualité croissante** :

| Couche | Rôle | Règle d'or | Ici |
|---|---|---|---|
| **Bronze** (raw) | le brut | on ne modifie **rien** | les pages CSV de l'API, telles quelles |
| **Silver** (clean) | le fiable | typé, dédoublonné, **contrôlé** | Parquet par jour + rejets avec leur raison |
| **Gold** (curated) | l'utile | **prêt pour la BI** | 4 tables de KPIs |

**Pourquoi c'est malin :** si une règle de nettoyage est fausse, on la corrige et on **rejoue** silver et gold à
partir du bronze, sans retélécharger. Et on peut toujours **remonter** d'un chiffre gold jusqu'aux lignes brutes.

### L'orchestration
Un **chef d'orchestre** qui lance chaque étape au bon moment, dans le bon ordre, la relance si elle échoue, et garde
l'historique. Les étapes et leurs dépendances forment un **DAG** (*Directed Acyclic Graph* : un graphe orienté sans
boucle).

### Le traitement distribué (Spark)
Quand les données sont trop grosses pour un script simple, **Spark** découpe le travail en morceaux (**partitions**)
traités **en parallèle**, sur plusieurs cœurs ou plusieurs machines.

### La qualité des données
Des **règles explicites** (une durée ne peut pas être négative…) appliquées à chaque ligne, des lignes invalides
**mises de côté** (pas supprimées en silence), un **rapport** chiffré, et une **porte qualité** qui arrête le pipeline
si la donnée est trop mauvaise.

---

## 3. Notre solution, exigence par exigence

```
 API de Chicago ──► BRONZE (CSV brut) ──Spark──► SILVER (propre) ──Spark──► GOLD (KPIs) ──► API SQL + tableau de bord
                    └─────────────────── tout dans le stockage objet (SeaweedFS, protocole S3) ───────────────┘
                    └─────────────────── chaque étape = une tâche Airflow, tout lancé par Docker Compose ──────┘
```

| # | Exigence | Notre réponse | Où |
|---|---|---|---|
| 1 | Ingestion dans le datalake | téléchargement par l'API SODA, un mois par tâche, pages de 50 000 lignes, relances, **manifeste** par mois (relancer ne retélécharge pas) | `pipeline/ingest/soda.py` |
| 2 | Nettoyage | schéma explicite, typage, **7 règles nommées**, dédoublonnage par `trip_id`, colonnes enrichies, rejets gardés avec leur raison, **porte qualité** à 20 % | `pipeline/spark/bronze_to_silver.py` |
| 3 | Agrégats BI | `daily_kpis`, `top_pickup_zones` (jointure avec les noms de zones), `hourly_demand`, `payment_mix` ; API SQL/JSON et tableau de bord | `pipeline/spark/silver_to_gold.py`, `api/` |
| 4 | Couches bronze → silver → gold | trois préfixes du bucket, chacun matérialisé | `pipeline/config.py` |
| 5 | Stockage objet | SeaweedFS (compatible S3), Spark via S3A, Python via boto3 | `docker-compose.yml` |
| 6 | Orchestration | Airflow : un DAG, une tâche par mois (*dynamic mapping*), contrôles, relances | `dags/taxi_pipeline.py` |
| 7 | Spark | PySpark 3.5.4 en mode local, lancé par `spark-submit` | `pipeline/spark/` |
| 8 | Une commande | `docker compose up -d --build` : le DAG démarre seul (`@once`) | `docker-compose.yml` |
| 9 | Reproductible | période fixée dans `.env` (T1 2023 = 1 446 439 trajets), requête documentée, script de téléchargement autonome | `.env.example`, `scripts/download_trips.py` |
| 10 | Livrables | dépôt GitHub, README (lancement, schéma, choix, suite), documentation en français | `README.md`, `docs/` |

**Le résultat, mesuré** (lancement complet du 29/10/2026) : 1 446 439 lignes ingérées, 1 416 392 gardées, 30 047
rejetées (2,08 %, surtout des durées nulles), 4 tables gold, environ 8 minutes de traitement après la construction des
images. Revenu du trimestre : 36,46 M$ ; durée moyenne : 19,4 min ; première zone : Near North Side.

---

## 4. Pourquoi Docker (et pourquoi chaque outil)

### Pourquoi Docker ?
Notre solution assemble **sept logiciels** : Python, Java, Spark, Airflow, PostgreSQL, SeaweedFS, une API. Sans
Docker, quelqu'un qui veut la lancer devrait installer chacun, **aux bonnes versions**, configurer qu'ils se trouvent,
et prier pour que ça marche sur son système. Docker règle ça :

1. **Reproductibilité** : chaque logiciel est emballé dans une **image** avec ses versions exactes (Airflow 2.10.5,
   Spark 3.5.4, Java 17…). La même image donne le même résultat partout : sur ton PC, celui du correcteur, un serveur.
2. **Isolation** : chaque service tourne dans son **conteneur**, sans polluer la machine ni entrer en conflit (deux
   versions de Java, par exemple).
3. **Une seule commande** : avec Docker Compose, `docker compose up` construit, démarre dans l'ordre et relie tous les
   services. C'est une exigence du sujet.
4. **Proche de la production** : en entreprise, les pipelines tournent dans des conteneurs (Kubernetes, services
   cloud). Le même découpage vaut en local et en production.
5. **Jetable** : `docker compose down -v` efface tout proprement ; on peut recommencer de zéro en une minute.

**Image, conteneur, volume en une image mentale :** l'**image** est une *recette* (le Dockerfile) cuite une fois ; le
**conteneur** est le *plat* servi à partir de cette recette (on peut en servir plusieurs) ; le **volume** est le
*frigo* où l'on garde ce qui doit survivre quand on débarrasse (les données du lac).

### Pourquoi chaque autre outil ?

| Outil | Pourquoi lui | Alternative possible, et pourquoi pas ici |
|---|---|---|
| **Airflow** | le plus répandu chez les clients, interface claire, relances, tâches dynamiques | Dagster : plus moderne, centré sur les données ; Airflow reste la référence attendue en entretien |
| **Spark (local)** | imposé ; passe à l'échelle sans changer le code | un cluster Spark dans Docker : plus de mémoire pour aucun gain à ce volume |
| **SeaweedFS** | compatible S3, image libre disponible | MinIO : le premier choix, mais ses images et binaires gratuits ont été retirés en 2025 |
| **Parquet** | colonnes, types, compression (644 Mo de CSV → 120 Mo) | CSV partout : lent et non typé ; Delta/Iceberg : mieux, prévu « avec plus de temps » |
| **PostgreSQL** | base recommandée pour Airflow | SQLite : interdit en mode parallèle d'Airflow |
| **DuckDB + FastAPI** | SQL directement sur le Parquet du lac, sans copier dans une base | Trino/Presto : trop lourd en local ; Metabase : un tableau de bord de plus à installer |

---

## 5. Ce qu'il faut savoir pour défendre la solution

### Questions probables en entretien, et réponses courtes

* **« Pourquoi garder le brut ? »** Pour rejouer sans retélécharger, auditer, et corriger une règle après coup.
* **« Que se passe-t-il si l'API tombe ? »** Relances avec attente croissante (6 essais), puis la tâche du mois échoue
  seule ; Airflow la réessaie ; les autres mois ne sont pas touchés ; le manifeste empêche un mois incomplet de passer
  pour complet.
* **« Et si les données sont mauvaises ? »** Règles nommées, rejets conservés avec leur raison, rapport qualité, et
  arrêt du pipeline au-delà de 20 % de rejets.
* **« Comment évite-t-on les doublons ? »** Fenêtre Spark par `trip_id`, on garde la ligne de l'ingestion la plus
  récente ; les autres copies vont dans les rejets (`duplicate`).
* **« Relancer, ça duplique ? »** Non : le pipeline est **idempotent** (les mois déjà ingérés sont sautés, silver et
  gold sont réécrits).
* **« Ça passe à l'échelle ? »** Le code Spark est le même sur un cluster (changer `SPARK_MASTER`) ; le stockage S3
  est illimité ; il faudrait ajouter le traitement incrémental (seulement les nouveaux jours).
* **« Qu'est-ce qui a cassé, et qu'as-tu appris ? »** Trois choses réelles : conflit de versions PySpark avec Airflow ;
  MinIO retiré de Docker Hub ; Spark qui échoue à renommer ses fichiers sur un stockage objet, résolu par le committer
  « magic » de S3A. Leçon : on ne sait qu'un pipeline marche qu'en le lançant de bout en bout.
* **« Avec plus de temps ? »** Delta Lake ou Iceberg (transactions, historique), traitement incrémental quotidien,
  contrôles de qualité outillés (Great Expectations, Soda), CI, dbt pour la couche métier, un vrai cluster.

### Vocabulaire à maîtriser
Datalake, data warehouse, ETL/ELT, architecture médaillon, idempotence, partitionnement, format colonnaire, DAG,
orchestrateur, executor, driver Spark, transformation paresseuse (*lazy*), action, shuffle, jointure *broadcast*,
fonction de fenêtre, porte qualité, conteneur, image, volume, réseau Docker, healthcheck.

---

## 6. Ressources pour apprendre

**Datalake et architecture médaillon**
* Databricks — *What is a medallion architecture?* : https://www.databricks.com/glossary/medallion-architecture
* AWS — *What is a data lake?* : https://aws.amazon.com/what-is/data-lake/
* Livre : *Fundamentals of Data Engineering*, Joe Reis & Matt Housley (O'Reilly, 2022) — la meilleure vue d'ensemble du
  métier.

**Docker et Docker Compose**
* Docker — *Get started* (officiel, avec exercices) : https://docs.docker.com/get-started/
* Docker Compose — documentation : https://docs.docker.com/compose/
* Référence du fichier Compose (`depends_on`, `healthcheck`, `volumes`) : https://docs.docker.com/reference/compose-file/

**Apache Airflow**
* Tutoriel officiel (DAG, tâches, TaskFlow) : https://airflow.apache.org/docs/apache-airflow/stable/tutorial/index.html
* Dynamic task mapping (`.expand()`) : https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html
* Airflow avec Docker Compose (officiel) : https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

**Apache Spark / PySpark**
* Guide de démarrage PySpark : https://spark.apache.org/docs/latest/api/python/getting_started/index.html
* Spark SQL et DataFrames : https://spark.apache.org/docs/latest/sql-programming-guide.html
* Spark sur stockage objet et committers S3A : https://spark.apache.org/docs/latest/cloud-integration.html
* Livre : *Learning Spark* (2ᵉ édition), Damji et al. (O'Reilly) — gratuit sur le site de Databricks.

**Stockage et formats**
* Apache Parquet : https://parquet.apache.org/docs/
* SeaweedFS (API S3) : https://github.com/seaweedfs/seaweedfs/wiki/Amazon-S3-API
* boto3 (S3 en Python) : https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3.html

**Exposition**
* DuckDB — lire du Parquet sur S3 : https://duckdb.org/docs/guides/network_cloud_storage/s3_import
* FastAPI — tutoriel : https://fastapi.tiangolo.com/tutorial/

**La donnée source**
* Taxi Trips (2013-2023), Chicago : https://data.cityofchicago.org/Transportation/Taxi-Trips-2013-2023-/wrvz-psew
* L'API SODA (filtres, pagination) : https://dev.socrata.com/docs/queries/

**Git et GitHub**
* *Pro Git* (livre gratuit, en français) : https://git-scm.com/book/fr/v2
* GitHub CLI (`gh auth login`) : https://cli.github.com/manual/gh_auth_login

---

## Où lire la suite

* [`INSTALLATION.md`](INSTALLATION.md) — l'installer et le lancer sur un autre ordinateur ;
* [`PAS-A-PAS.md`](PAS-A-PAS.md) — ce qui se passe à chaque étape d'une exécution ;
* [`TECHNOLOGIES.md`](TECHNOLOGIES.md) — chaque outil en détail et où il est dans le code ;
* [`GUIDE.md`](GUIDE.md) — les choix, la construction, Docker, Compose, Git.
