# Guide complet du projet (en français)

Ce guide explique **ce que fait le projet, pourquoi il est construit ainsi, comment il a été construit, comment le
lancer avec Docker et Docker Compose, et comment le code a été envoyé sur GitHub**. Le `README.md` (en anglais) reste
le résumé pour le jury ; ce guide est l'explication détaillée.

Sommaire
1. Le projet en une page
2. Les données
3. L'architecture et les choix (le « pourquoi »)
4. Comment le projet a été construit (le « comment », étape par étape)
5. Docker expliqué avec ce projet
6. Docker Compose expliqué avec ce projet
7. Lancer, vérifier, relancer, dépanner
8. Git et GitHub : ce qui a été fait, et comment pousser ensuite
9. Glossaire

---

## 1. Le projet en une page

Une entreprise de mobilité veut des chiffres fiables sur ses trajets : revenus par jour, durée des trajets, zones de
prise en charge les plus actives. Le projet construit le **pipeline de données** qui produit ces chiffres à partir des
données brutes, de façon **automatique, reproductible et contrôlée** :

1. **Ingestion** : on télécharge les trajets de taxi de Chicago (API publique SODA) et on les dépose **tels quels**
   dans un stockage objet (SeaweedFS, l'équivalent local d'Amazon S3). C'est la couche **bronze**.
2. **Nettoyage** : Spark lit le bronze, convertit les types, applique des règles de qualité, retire les doublons,
   met de côté les lignes invalides avec leur raison, et écrit des fichiers Parquet propres. C'est la couche **silver**.
3. **Agrégation** : Spark calcule les tables métier (KPIs) pour la BI. C'est la couche **gold**.
4. **Exposition** : une petite API (FastAPI + DuckDB) permet d'interroger le gold en SQL ou en JSON, et un tableau de
   bord affiche les principaux indicateurs.
5. **Orchestration** : Airflow enchaîne ces étapes dans le bon ordre (un « DAG »), gère les relances et les journaux.
6. **Tout tourne avec une seule commande** : `docker compose up -d --build`.

---

## 2. Les données

* Jeu de données : *Taxi Trips (2013-2023)* de la ville de Chicago, identifiant `wrvz-psew`.
* On ne télécharge pas tout (plusieurs dizaines de Go) : on prend **le premier trimestre 2023**
  (du 1ᵉʳ janvier inclus au 1ᵉʳ avril exclu). Mesuré par l'API (`$select=count(*)` sur ce filtre) :
  **1 446 439 trajets**.
* Téléchargement par **pages** de 50 000 lignes : filtre sur `trip_start_timestamp`, tri par l'identifiant interne
  `:id` (pour que la pagination ne « saute » ni ne répète aucune ligne), `$limit` et `$offset`.
* Le script `scripts/download_trips.py` fait le même téléchargement hors Docker, vers des fichiers locaux.
* Les 77 *community areas* (zones) de Chicago sont fournies dans `seeds/community_areas.csv` pour donner un nom
  aux numéros de zone.

---

## 3. L'architecture et les choix (le « pourquoi »)

```
 API SODA ──► BRONZE (CSV brut, par mois) ──Spark──► SILVER (Parquet propre + rejets) ──Spark──► GOLD (KPIs) ──► API / tableau de bord
                        └──────────────── tout est stocké dans SeaweedFS (s3://datalake) ─────────────────┘
                        └──────────────── chaque étape est une tâche Airflow ────────────────────────┘
```

**Pourquoi trois couches (médaillon) ?**
* **Bronze = la vérité brute.** On garde exactement ce que la source a envoyé. Si une règle de nettoyage était
  fausse, on corrige et on rejoue silver **sans retélécharger**.
* **Silver = la donnée fiable.** Typée, dédoublonnée, contrôlée. Les lignes invalides ne disparaissent pas : elles vont
  dans `taxi_trips_rejected` avec la liste de leurs raisons. On peut toujours expliquer un chiffre.
* **Gold = la donnée utile.** Des tables petites et prêtes à l'emploi pour la BI.

**Pourquoi Airflow ?** C'est l'orchestrateur le plus répandu chez les clients. Il donne : un graphe de tâches
(DAG) avec des dépendances, une tâche par mois (« dynamic task mapping ») qui peut être relancée seule, des relances
automatiques, une interface pour voir chaque journal.

**Pourquoi Spark en mode local ?** Le sujet impose Spark. Pour quelques millions de lignes, un cluster Spark
dans Docker coûterait beaucoup de mémoire pour rien. Les traitements sont des scripts `spark-submit` normaux : les
envoyer sur un vrai cluster ne demande que de changer `SPARK_MASTER`.

**Pourquoi SeaweedFS ?** Un datalake repose sur un stockage objet. Le premier choix était MinIO, mais ses images Docker et
ses binaires gratuits ont été retirés en 2025 (`docker pull minio/minio` refusé, `dl.min.io` répond 410). SeaweedFS
parle le même protocole qu'Amazon S3 : passer
à S3, GCS ou Azure plus tard, c'est de la configuration. Spark y accède avec le connecteur S3A (les fichiers `.jar`
sont mis dans l'image Docker, rien n'est téléchargé au lancement).

**Pourquoi Parquet ?** Format en colonnes, typé, compressé : beaucoup plus rapide et léger que le CSV à relire. Le
silver est partitionné par jour (`trip_date=...`) : relancer un traitement ne réécrit que les jours concernés.

**Pourquoi une « porte qualité » ?** Si plus de 20 % des lignes sont rejetées, la tâche silver **échoue** et le gold
n'est pas recalculé : une source cassée doit arrêter le pipeline, pas envoyer de faux chiffres à la BI. Le seuil est
réglable (`MAX_REJECT_RATE`).

**Pourquoi DuckDB pour l'exposition ?** DuckDB lit le Parquet directement dans SeaweedFS, en SQL, sans charger une base
de données de plus. L'API FastAPI ne fait qu'envelopper ces requêtes (le point `/sql` n'accepte que des lectures).

**Les règles de qualité (silver)**, chacune avec son nom :

| Règle | Une ligne est rejetée si… |
|---|---|
| `missing_trip_id` | pas d'identifiant de trajet |
| `missing_start` | date de début absente ou illisible |
| `start_outside_window` | le trajet commence hors de la période demandée |
| `end_before_start` | fin absente ou avant le début |
| `bad_duration` | durée ≤ 0 s ou > 24 h |
| `bad_distance` | distance < 0 ou > 500 miles |
| `bad_amount` | montant total absent ou négatif, tarif négatif, ou total > 1 000 $ |
| `duplicate` | une autre copie du même `trip_id` a été gardée (la plus récente) |

**Les tables gold :**

| Table | Une ligne par… | Contenu |
|---|---|---|
| `daily_kpis` | jour | trajets, revenu, tarifs, pourboires, total moyen, durée moyenne / médiane / p90, distance, part carte bancaire |
| `top_pickup_zones` | zone de prise en charge | nom de la zone, trajets, part, revenu, durée et total moyens, rang |
| `hourly_demand` | jour de la semaine × heure | trajets, durée moyenne, total moyen |
| `payment_mix` | moyen de paiement | trajets, revenu, taux de pourboire moyen |

---

## 4. Comment le projet a été construit (étape par étape)

1. **Mesurer avant de coder.** Une requête `count(*)` sur l'API pour le T1 2023 : 1 446 439 trajets, un volume
   raisonnable. Une requête de 2 lignes pour voir le format exact (en-têtes, dates `2023-01-01T23:45:00.000`).
2. **Le stockage et la configuration** (`pipeline/config.py`, `pipeline/storage.py`) : tous les réglages viennent
   de variables d'environnement (fichier `.env`), et les chemins du datalake sont définis à un seul endroit.
3. **L'ingestion bronze** (`pipeline/ingest/soda.py`) : un mois à la fois, page par page, avec relances en cas
   d'erreur de l'API (429, 5xx). Chaque mois se termine par un `_manifest.json` (requête, nombre de lignes, durée).
   Un mois déjà complet est sauté : relancer le DAG ne retélécharge rien.
4. **Les traitements Spark** (`pipeline/spark/`) : chaque étape est une fonction pure (typer, vérifier, dédoublonner,
   enrichir, agréger), facile à tester sur de petites données.
5. **Les tests** (`tests/`) : 8 tests (pagination, chaque règle de qualité, doublons, porte qualité, tables gold).
   Résultat : 8 réussis, avec Spark en local.
6. **Validation sur de vraies données** : 22 714 trajets réels (2 et 3 janvier 2023) passés dans le silver et le gold.
   2,2 % rejetés (479 durées invalides, 16 montants, 7 distances, 3 fins avant début) ; zone n°1 : O'Hare (22,8 %).
7. **L'orchestration** (`dags/taxi_pipeline.py`) : `plan_months` → `ingest_month` (une tâche par mois) →
   `check_bronze` + `ingest_reference` → `bronze_to_silver` → `silver_to_gold`.
8. **L'exposition** (`api/`) : JSON, SQL, tableau de bord.
9. **Docker** : une image Airflow avec Java et PySpark, une image pour l'API, et un `docker-compose.yml` qui démarre
   tout, avec des plafonds de mémoire pour chaque conteneur.
10. **Le lancement complet**, et les trois vrais problèmes qu'il a révélés (c'est pour ça qu'on teste en vrai) :
    * **Conflit de versions** : Airflow 2.10.5 impose PySpark 3.5.4 (ses « constraints ») et le projet demandait 3.5.3 →
      `pip` refusait d'installer. Corrigé en passant à 3.5.4 (tests relancés : 8/8).
    * **MinIO retiré** : `docker pull minio/minio` refusé, binaires en 410 → remplacé par SeaweedFS (même protocole S3,
      aucun code à changer, seulement le `docker-compose.yml`).
    * **Écriture Spark sur stockage objet** : Spark termine ses écritures en *renommant* des dossiers temporaires ; sur un
      stockage objet un renommage est une copie + suppression, et il échouait (`Could not rename s3a://…`). Corrigé avec
      le **committer « magic » de S3A** (jar `spark-hadoop-cloud`) : chaque fichier est écrit directement à sa place.
      Seules les deux tâches Spark ont été relancées (`airflow tasks clear`), l'ingestion déjà faite a été gardée.

---

## 5. Docker expliqué avec ce projet

**Le problème que Docker résout.** Le projet a besoin de Python 3.11, de Java, de Spark, d'Airflow, de Postgres, de
SeaweedFS… Les installer à la main sur chaque machine, avec les bonnes versions, est long et fragile. Docker emballe chaque
logiciel avec tout ce dont il a besoin, et ça tourne pareil partout.

**Les mots à connaître :**

| Mot | Ce que c'est | Dans ce projet |
|---|---|---|
| **Image** | un modèle figé : un système minimal + des logiciels installés | `taxi-airflow:local` (Airflow + Java + Spark), `chrislusf/seaweedfs`, `postgres:16-alpine` |
| **Dockerfile** | la recette qui construit une image, ligne par ligne | `docker/airflow/Dockerfile`, `docker/api/Dockerfile` |
| **Conteneur** | une image en train de tourner (un processus isolé) | le scheduler Airflow, SeaweedFS, Postgres, l'API |
| **Volume** | un espace disque qui survit à l'arrêt des conteneurs | `s3-data` (le datalake), `postgres-data` (l'historique Airflow) |
| **Port** | une porte entre ta machine et un conteneur | `8080` (Airflow), `8888` (navigateur du datalake), `8333` (S3), `8000` (API) |
| **Montage (bind mount)** | un dossier de ta machine vu à l'intérieur du conteneur | `./dags`, `./pipeline` : on modifie le code sans reconstruire l'image |

**Lire notre Dockerfile Airflow :**

```dockerfile
FROM apache/airflow:2.10.5-python3.11          # on part de l'image officielle d'Airflow
USER root                                      # droits d'administrateur pour installer Java
RUN apt-get install -y default-jre-headless    # Java, nécessaire à Spark
ENV JAVA_HOME=/usr/lib/jvm/default-java
RUN curl ... hadoop-aws-3.3.4.jar ...           # connecteur S3A : Spark peut lire/écrire dans SeaweedFS
USER airflow                                   # on revient à l'utilisateur normal (sécurité)
RUN pip install ... -r requirements.txt        # PySpark, boto3, requests
```

**Commandes Docker utiles :**

```bash
docker images                     # les images présentes sur la machine
docker ps                         # les conteneurs qui tournent
docker logs <conteneur>           # le journal d'un conteneur
docker exec -it <conteneur> bash  # ouvrir un terminal dans un conteneur
docker stats                      # mémoire et CPU de chaque conteneur, en direct
docker system df                  # place disque prise par Docker
```

> Sur Linux, ajoute `sudo` devant si ton utilisateur n'est pas dans le groupe `docker`
> (`sudo usermod -aG docker $USER`, puis reconnecte-toi).

---

## 6. Docker Compose expliqué avec ce projet

**Le problème.** Le projet n'a pas un conteneur mais sept, qui doivent démarrer **dans le bon ordre**, se parler et
partager des réglages. Docker Compose décrit tout ça dans **un seul fichier** (`docker-compose.yml`) et le lance avec
**une seule commande**.

**Nos services :**

| Service | Rôle | Démarre quand… |
|---|---|---|
| `s3` | le stockage objet (datalake) | tout de suite ; « healthy » quand il répond |
| `bucket-init` | crée le bucket `datalake`, puis s'arrête | SeaweedFS est « healthy » |
| `postgres` | la base interne d'Airflow (historique des exécutions) | tout de suite |
| `airflow-init` | prépare la base d'Airflow et crée l'utilisateur admin, puis s'arrête | Postgres est prêt et le bucket existe |
| `airflow-webserver` | l'interface web d'Airflow (port 8080) | `airflow-init` a réussi |
| `airflow-scheduler` | lance les tâches ; **Spark tourne ici** | `airflow-init` a réussi |
| `api` | l'API et le tableau de bord (port 8000) | le bucket existe |

**Les notions du fichier :**

* `build:` construit une image à partir d'un Dockerfile ; `image:` utilise une image toute faite.
* `x-airflow-common` + `<<: *airflow-common` : un bloc écrit une fois et réutilisé par les 3 services Airflow
  (même image, mêmes variables, mêmes montages).
* `env_file: .env` : les réglages (période, mots de passe, seuils) viennent du fichier `.env`.
* `depends_on` + `condition` : l'ordre de démarrage. `service_healthy` = attendre que le test de santé passe ;
  `service_completed_successfully` = attendre qu'un service de préparation ait fini sans erreur.
* `healthcheck` : la commande qui dit si un service est prêt (un appel HTTP au point S3 pour SeaweedFS, `pg_isready` pour Postgres).
* `volumes` : en bas du fichier, les volumes nommés (les données qui restent après un arrêt).
* `mem_limit` : un plafond de mémoire par conteneur. S'il est dépassé, seul ce conteneur est arrêté, pas la machine.
* `restart: unless-stopped` : le service redémarre tout seul s'il plante.
* Les conteneurs se parlent par **leur nom de service** : Spark écrit dans `http://s3:8333`, Airflow lit
  `postgres:5432`.

**Commandes Docker Compose (à lancer dans le dossier du projet) :**

```bash
docker compose up -d --build      # construit les images si besoin et démarre tout en arrière-plan
docker compose ps                 # l'état de chaque service
docker compose logs -f airflow-scheduler   # suivre le journal d'un service (Ctrl+C pour sortir)
docker compose exec airflow-scheduler bash # un terminal dans le scheduler
docker compose exec airflow-scheduler airflow dags trigger taxi_pipeline   # relancer le pipeline
docker compose restart api        # redémarrer un seul service
docker compose down               # tout arrêter (les données restent dans les volumes)
docker compose down -v            # tout arrêter ET effacer les volumes (datalake et historique Airflow)
docker compose build --no-cache   # reconstruire les images de zéro
```

Le `Makefile` donne des raccourcis : `make up`, `make trigger`, `make status`, `make logs`, `make test`,
`make down`, `make clean`.

---

## 7. Lancer, vérifier, relancer, dépanner

**Lancer :**

```bash
git clone https://github.com/omarajraoui/artefact-taxi.git
cd artefact-taxi
cp .env.example .env
docker compose up -d --build
```

Le premier `up` construit les images (5 à 15 minutes), puis le DAG démarre seul.

**Vérifier chaque couche :**

1. **Airflow** — http://localhost:8080 (`admin` / `admin`) → DAG `taxi_pipeline` → vue *Graph* : chaque tâche passe
   au vert. Cliquer sur une tâche → *Logs* pour voir ce qu'elle a fait (pages téléchargées, rapport qualité…).
2. **Le datalake** — http://localhost:8888/buckets/datalake/ (navigateur de fichiers SeaweedFS) → dossiers `bronze/`,
   `silver/`, `gold/`. Le rapport qualité : `silver/_quality/latest.json`.
3. **API** — http://localhost:8000 (tableau de bord), http://localhost:8000/docs (tous les points d'accès),
   http://localhost:8000/quality (rapport qualité).

**Relancer :** `make trigger`. Pour retélécharger les mois : dans Airflow, *Trigger DAG w/ config* avec
`{"force_ingest": true}`. Pour un essai rapide : `MAX_ROWS_PER_MONTH=20000` dans `.env`, puis
`docker compose up -d` et `make trigger`.

**Dépanner :**

| Symptôme | Cause probable | Que faire |
|---|---|---|
| `port is already allocated` | un autre logiciel utilise 8080, 8000, 8333 ou 8888 | arrêter ce logiciel, ou changer le port de gauche dans `ports:` |
| une tâche Spark échoue avec `exit code 137` | plus de mémoire dans le conteneur scheduler | baisser `SPARK_DRIVER_MEMORY` ou monter `SCHEDULER_MEM_LIMIT` dans `.env` |
| `ingest_month` en *up_for_retry* | l'API SODA limite le débit (429) | attendre (relances automatiques) ou mettre un `SODA_APP_TOKEN` |
| `bronze_to_silver` échoue « quality gate failed » | plus de 20 % de lignes rejetées | lire `silver/_quality/latest.json` : quelle règle ? |
| le tableau de bord dit « Gold tables not built yet » | le DAG n'a pas fini | attendre la fin de `silver_to_gold` |

---

## 8. Git et GitHub : ce qui a été fait, et comment pousser ensuite

**Git** garde l'historique du code sur ta machine. **GitHub** est le serveur où l'on dépose une copie de cet
historique pour la partager. « Pousser » (`git push`) = envoyer tes commits locaux vers GitHub.

**Ce qui a été fait pour ce dépôt, dans l'ordre :**

1. `git init` dans `~/artefact-taxi` : le dossier devient un dépôt Git.
2. `git add -A` puis `git commit -m "..."` : un premier **commit** (une photo de tous les fichiers), signé à ton nom.
3. Tu as créé le dépôt vide `omarajraoui/artefact-taxi` sur github.com.
4. `git remote add origin https://github.com/omarajraoui/artefact-taxi.git` : on dit à Git où se trouve la copie
   GitHub (`origin` est le nom habituel de ce lien).
5. **Le problème du mot de passe** : GitHub n'accepte plus les mots de passe pour `git push` depuis 2021. Il faut un
   *token* ou une connexion par l'outil officiel `gh`.
6. **La solution utilisée** : `gh auth login --web` sur la VM. L'outil affiche un code à usage unique ; tu l'as saisi
   sur https://github.com/login/device et tu as cliqué *Authorize*. La VM est alors reconnue comme toi.
   `gh auth setup-git` dit à Git d'utiliser cette connexion.
7. `git push -u origin main` : envoi de la branche `main` vers GitHub (`-u` retient le lien, les prochaines fois
   `git push` suffit).

8. **Deuxième envoi** (après le lancement complet) : les corrections (PySpark 3.5.4, SeaweedFS, committer « magic »)
   et ce guide ont été ajoutés avec `git add -A`, `git commit -m "..."`, puis `git push`. Plus besoin de `-u` ni de se
   reconnecter : la connexion `gh` est gardée.

**Pour envoyer tes prochaines modifications :**

```bash
cd ~/artefact-taxi
git status                 # ce qui a changé
git diff                   # le détail des changements
git add <fichiers>         # choisir ce qui part dans le prochain commit (ou git add -A pour tout)
git commit -m "Ce que j'ai changé, en une phrase"
git push                   # envoyer vers GitHub
```

**Bonnes pratiques :**
* Ne jamais committer de secrets : `.env` est dans `.gitignore`, seul `.env.example` (sans vrai secret) est versionné.
* Un commit = un changement cohérent, avec un message clair.
* Pour un gros changement : une branche (`git switch -c ma-branche`), puis une *pull request* sur GitHub.
* La connexion `gh` est enregistrée sur la VM (`~/.config/gh`) et vaut pour tout ton compte GitHub. Pour la retirer :
  `gh auth logout`, ou github.com → Settings → Applications.

---

## 9. Glossaire

* **Datalake** : un stockage où l'on garde les données brutes et transformées, en fichiers, à bas coût.
* **Médaillon (bronze / silver / gold)** : organisation en trois couches, du brut au prêt à l'emploi.
* **DAG** : *Directed Acyclic Graph*, le graphe des tâches d'Airflow et de leurs dépendances.
* **Partition** : un sous-dossier par valeur (ex. `trip_date=2023-01-05`) pour ne lire que ce qui est utile.
* **Idempotent** : relancer donne le même résultat, sans doublon (ici : les mois déjà ingérés sont sautés).
* **Porte qualité (quality gate)** : un contrôle qui arrête le pipeline si la donnée est trop mauvaise.
* **S3 / S3A** : le protocole du stockage objet d'Amazon / le connecteur qui permet à Spark de l'utiliser.
* **KPI** : indicateur clé (revenu par jour, durée moyenne…).

---

## Résultat du lancement complet (mesuré le 29/10/2026, VM 4 cœurs / 5,8 Go)

| Étape | Résultat | Durée |
|---|---|---|
| `ingest_month` × 3 | 1 446 439 lignes en 30 pages SODA (janvier, février, mars en parallèle) | ~3 min |
| `bronze_to_silver` | 1 416 392 propres, 30 047 rejetées (2,08 %) : 28 767 `bad_duration`, 1 347 `bad_amount`, 36 `end_before_start`, 25 `bad_distance` ; porte qualité passée | 3 min 30 s |
| `silver_to_gold` | `daily_kpis` 90 lignes, `top_pickup_zones` 78, `hourly_demand` 168, `payment_mix` 7 | 1 min 10 s |

Sur le T1 2023 (trajets propres) : **36,46 M$ de revenu**, **19,4 min** de durée moyenne. Zones de prise en charge
n°1 à 3 : Near North Side (21,1 % des trajets), O'Hare (16,9 %, le plus gros revenu : 12,5 M$), Loop (15,9 %).
Chaque conteneur est resté sous son plafond de mémoire.
