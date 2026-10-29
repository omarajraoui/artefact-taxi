# Chaque technologie : ce que c'est, à quoi elle sert ici, où la trouver dans le code

Pour chaque outil : **ce que c'est** (en une phrase simple), **pourquoi on l'utilise**, **comment il est utilisé dans
ce projet** (avec les fichiers), et **ce qu'il faut retenir pour un entretien**.

---

## L'API SODA de Chicago (la source)

**Ce que c'est.** La ville de Chicago publie ses données ouvertes sur la plateforme Socrata ; l'API « SODA » permet de
les interroger par URL, un peu comme du SQL : `$where` (filtre), `$order` (tri), `$limit` / `$offset` (pagination),
`$select=count(*)` (compter).

**Ici.** `pipeline/ingest/soda.py` construit, pour chaque mois, des requêtes du type :
```
https://data.cityofchicago.org/resource/wrvz-psew.csv
  ?$where=trip_start_timestamp >= '2023-01-01T00:00:00' AND trip_start_timestamp < '2023-02-01T00:00:00'
  &$order=:id &$limit=50000 &$offset=0, 50000, 100000…
```
* `$order=:id` : on trie par l'identifiant interne de la ligne pour que les pages soient **stables** (sans tri, deux
  pages successives pourraient se chevaucher ou sauter des lignes).
* En cas d'erreur 429 (trop de requêtes) ou 5xx, on attend de plus en plus longtemps et on réessaie (jusqu'à 6 fois).

**À retenir.** On ne télécharge pas le fichier entier (des dizaines de Go) : on filtre côté serveur et on pagine.

---

## Python

**Ce que c'est.** Le langage de tout le projet : ingestion, traitements Spark (PySpark), DAG Airflow, API.

**Ici.** Le code est un **paquet** `pipeline/` (importable partout) : `config.py` (réglages lus dans les variables
d'environnement), `storage.py` (fonctions S3), `ingest/` (bronze), `spark/` (silver et gold). Les DAG et l'API
importent ce paquet plutôt que de dupliquer le code.

---

## Docker

**Ce que c'est.** Un outil qui emballe un logiciel avec tout ce dont il a besoin (système, bibliothèques, versions)
dans une **image**, puis le fait tourner dans un **conteneur** isolé, identique sur toutes les machines.

**Ici.** Deux images construites par le projet :
* `docker/airflow/Dockerfile` → `taxi-airflow:local` : l'image officielle d'Airflow + **Java** (Spark en a besoin) +
  **PySpark** + les **jars** qui permettent à Spark de parler au stockage S3 (`hadoop-aws`, `aws-java-sdk-bundle`,
  `spark-hadoop-cloud`), téléchargés au moment de la construction, pas au lancement.
* `docker/api/Dockerfile` → `taxi-api:local` : Python léger + FastAPI + DuckDB (et son extension S3 préinstallée).

Et trois images toutes faites : `chrislusf/seaweedfs` (stockage), `postgres:16-alpine` (base d'Airflow).

**À retenir.** « Ça marche sur ma machine » devient « ça marche partout » : la recette (Dockerfile) est versionnée
avec le code.

---

## Docker Compose

**Ce que c'est.** Un fichier (`docker-compose.yml`) qui décrit **plusieurs conteneurs ensemble** : lesquels, dans quel
ordre, avec quels réglages, quels disques et quels ports. Une commande les démarre tous : `docker compose up`.

**Ici.** Sept services (`s3`, `bucket-init`, `postgres`, `airflow-init`, `airflow-webserver`, `airflow-scheduler`,
`api`). Les points clés :
* **ordre de démarrage** avec `depends_on` + `condition: service_healthy` (attendre qu'un service soit prêt) ou
  `service_completed_successfully` (attendre qu'une préparation soit finie) ;
* **réglages** partagés par `env_file: .env` ;
* **réseau** : chaque service est joignable par son nom (`http://s3:8333`, `postgres:5432`) ;
* **volumes** : `s3-data` (le datalake) et `postgres-data` survivent à un `docker compose down` ;
* **plafonds de mémoire** `mem_limit` : si Spark dépasse, seul son conteneur est arrêté, pas la machine.

---

## SeaweedFS (le stockage objet = le socle du datalake)

**Ce que c'est.** Un stockage de fichiers compatible avec le protocole **S3** d'Amazon : on y range des « objets »
(fichiers) dans des « buckets » (conteneurs), adressés par une clé (`bronze/taxi_trips/month=2023-01/page=00001.csv`).

**Pourquoi.** Un datalake se construit sur un stockage objet (S3, GCS, Azure Blob) : bon marché, illimité, lisible par
tous les outils. SeaweedFS est l'équivalent local. Le premier choix était MinIO, mais ses images et binaires gratuits
ont été retirés en 2025 (vérifié : `docker pull minio/minio` refusé, `dl.min.io` répond 410). Comme tout le code parle
S3, changer de stockage n'a demandé que de modifier le `docker-compose.yml`.

**Ici.** Le service `s3` écoute sur le port **8333** (protocole S3) et **8888** (navigateur de fichiers). Le service
`bucket-init` crée le bucket `datalake`. Les identifiants viennent de `.env` (`S3_ACCESS_KEY`, `S3_SECRET_KEY`).

---

## boto3

**Ce que c'est.** La bibliothèque Python officielle pour parler à S3.

**Ici.** `pipeline/storage.py` : déposer les pages CSV en bronze, écrire et lire les manifestes et le rapport qualité
(JSON), effacer un mois à ré-ingérer. Utilisée aussi par `bucket-init` et l'API (`/quality`).

---

## Apache Airflow (l'orchestrateur)

**Ce que c'est.** Un outil qui exécute des **tâches** dans le bon ordre, selon un **DAG** (graphe de tâches), les
relance en cas d'échec, garde l'historique et les journaux, et offre une interface web.

**Ici.** `dags/taxi_pipeline.py` :
```
plan_months ─► ingest_month[janvier, février, mars] ─► check_bronze ─┐
                                    ingest_reference ─────────────────┴─► bronze_to_silver ─► silver_to_gold
```
* `plan_months` calcule la liste des mois de la période ;
* `ingest_month` est **dupliquée automatiquement** pour chaque mois (*dynamic task mapping*, `.expand()`) : trois
  tâches indépendantes, en parallèle (3 au plus), relançables séparément ;
* `check_bronze` arrête tout si un mois est vide (problème d'API ou de filtre) ;
* les deux tâches Spark sont des `BashOperator` qui lancent `spark-submit` ;
* `schedule="@once"` + DAG non suspendu à la création : il démarre tout seul au premier `up` ;
* `retries=2` : chaque tâche est réessayée deux fois avant d'être déclarée en échec ;
* un paramètre `force_ingest` permet de retélécharger.

Les composants d'Airflow, chacun dans un conteneur :
* **webserver** (port 8080) : l'interface ;
* **scheduler** : décide quand lancer chaque tâche et, avec le **LocalExecutor**, les exécute lui-même (c'est donc là
  que tourne Spark) ;
* **base de métadonnées** (Postgres) : l'état de chaque exécution et de chaque tâche ;
* **airflow-init** : prépare la base et crée l'utilisateur `admin`, une fois.

**À retenir.** Airflow **orchestre**, il ne traite pas les données lui-même : il lance Spark et vérifie que tout
s'est bien passé.

---

## PostgreSQL

**Ce que c'est.** Une base de données relationnelle.

**Ici.** Uniquement la **mémoire d'Airflow** (exécutions, états des tâches, utilisateurs). Les données de taxi n'y
sont pas : elles sont dans le datalake.

---

## Apache Spark / PySpark (le moteur de traitement)

**Ce que c'est.** Un moteur qui traite de gros volumes en découpant les données en **partitions** traitées en
parallèle. PySpark est son interface Python : on manipule des **DataFrames** (des tables) avec des fonctions
(`filter`, `groupBy`, `join`, `withColumn`…).

**Comment il fonctionne (l'essentiel).**
* Le **driver** est le programme principal ; il découpe le travail en **tâches** exécutées par des **executors**.
  En **mode local** (`local[*]`), driver et executors sont dans le même processus et utilisent tous les cœurs.
* Spark est **paresseux** (*lazy*) : `filter`, `withColumn`… ne calculent rien, ils construisent un plan. Le calcul a
  lieu à une **action** (`count`, `write`, `collect`). Spark optimise le plan entier avant de l'exécuter.
* `cache()` garde un résultat en mémoire quand il sert plusieurs fois (ici : le bronze lu, compté puis traité).

**Ici.**
* `pipeline/spark/session.py` : crée la session (mémoire, fuseau UTC, connexion S3A, committer « magic »).
* `pipeline/spark/bronze_to_silver.py` :
  1. lit tous les CSV du bronze avec un **schéma explicite tout en texte** (Spark ne devine rien) ;
  2. **type** les colonnes (`to_timestamp` avec le format `yyyy-MM-dd'T'HH:mm:ss.SSS`, `cast` en nombres) ;
  3. applique les **règles de qualité** : chaque ligne reçoit la liste des règles qu'elle viole (`reasons`) ;
  4. **dédoublonne** sur `trip_id` avec une **fenêtre** (`Window.partitionBy("trip_id")` + `row_number()`) ;
  5. **enrichit** (date, heure, jour, durée en minutes, vitesse, taux de pourboire) ;
  6. écrit le silver en **Parquet partitionné par jour**, et les rejets à part ;
  7. écrit le rapport qualité et **échoue** si plus de 20 % de rejets.
* `pipeline/spark/silver_to_gold.py` : `groupBy` + `agg` (sommes, moyennes, `percentile_approx` pour la médiane et le
  p90), **jointure** avec la table des zones (`broadcast` : la petite table est copiée partout, pas de mélange
  coûteux), `dense_rank` pour le classement.
* `spark-submit` lance chaque script avec les jars S3 (`--jars`) et la mémoire voulue (`--driver-memory`).

**Le connecteur S3A et le committer « magic ».** Spark lit et écrit `s3a://datalake/...` grâce au connecteur S3A.
Par défaut, Spark écrit dans un dossier temporaire puis **renomme** ; sur un stockage objet un renommage est une copie
puis une suppression, lent et fragile : notre premier lancement complet a échoué ainsi (`Could not rename s3a://…`).
Le **committer « magic »** fait écrire chaque fichier directement à sa place (envois en plusieurs parties, validés à
la fin) : plus de renommage. C'est la configuration recommandée pour Spark sur S3.

**À retenir.** Transformations pures et testables (fonctions `typed`, `with_reasons`, `dedup`, `enrich`, `build`),
exécutées par `spark-submit` : passer sur un vrai cluster ne demande que de changer `SPARK_MASTER`.

---

## Parquet (et CSV)

**CSV** : le format brut de la source, gardé tel quel en bronze (texte, lisible, mais lent et non typé).

**Parquet** : format **en colonnes**, **typé** et **compressé**. Lire une seule colonne ne lit qu'elle ; les types
(dates, nombres) sont conservés. Utilisé en silver et en gold.

**Partitionnement** : le silver est rangé en `trip_date=2023-01-05/…` ; une requête sur un jour ne lit que son dossier.

---

## DuckDB (requêtes SQL sur le gold)

**Ce que c'est.** Une base de données analytique **embarquée** (une bibliothèque, pas un serveur), très rapide sur le
Parquet, capable de lire directement dans S3 (extension `httpfs`).

**Ici.** `api/main.py` crée une **vue** par table gold
(`CREATE VIEW daily_kpis AS SELECT * FROM read_parquet('s3://datalake/gold/daily_kpis/*.parquet')`) : on interroge le
datalake en SQL sans rien copier dans une autre base.

---

## FastAPI + Uvicorn (l'API)

**Ce que c'est.** FastAPI : un framework Python pour écrire des API web ; il génère tout seul une documentation
interactive (`/docs`). Uvicorn : le serveur qui la fait tourner.

**Ici.** `api/main.py`, port 8000 :
* `GET /kpis/{table}` : une table gold en JSON ;
* `POST /sql` : une requête SQL **en lecture seule** (une seule instruction `SELECT` ou `WITH`) ;
* `GET /quality` : le dernier rapport qualité ;
* `GET /health`, `GET /tables` : l'état et le nombre de lignes de chaque table ;
* `GET /` : le tableau de bord.

---

## Chart.js (le tableau de bord)

**Ce que c'est.** Une bibliothèque JavaScript de graphiques.

**Ici.** `api/static/index.html` appelle l'API, affiche les chiffres clés, le revenu par jour, les durées (moyenne,
médiane, p90) et le top 10 des zones.

---

## pytest (les tests)

**Ce que c'est.** L'outil de tests de Python.

**Ici.** `tests/` : 8 tests sur de petites données créées en mémoire, avec un vrai Spark local :
pagination SODA, chaque règle de qualité, doublons, porte qualité, contenu des tables gold. Lancement : `make test`.

---

## Make

**Ce que c'est.** Un outil qui donne des noms courts à des commandes longues (`Makefile`).

**Ici.** `make up`, `make trigger`, `make status`, `make logs`, `make test`, `make down`, `make clean`.

---

## Git, GitHub et gh

**Git** garde l'historique du code (des « commits »). **GitHub** héberge une copie partagée. **gh** est l'outil officiel
de GitHub en ligne de commande ; ici il a servi à se connecter **sans mot de passe** (GitHub ne les accepte plus pour
`git push`) : un code à usage unique saisi sur https://github.com/login/device. Détails dans `GUIDE.md`, section 8.
