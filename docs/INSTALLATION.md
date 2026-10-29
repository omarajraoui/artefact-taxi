# Installer et lancer le projet sur un autre ordinateur

Ce document part de zéro : un ordinateur sans rien d'installé. Compter **30 à 45 minutes** la première fois
(installation de Docker + construction des images + exécution du pipeline).

## 0. Ce qu'il faut

| | Minimum | Conseillé |
|---|---|---|
| Mémoire (RAM) | 8 Go sur la machine, dont 5 Go donnés à Docker | 16 Go |
| Disque libre | 10 Go | 20 Go |
| Processeur | 4 cœurs | 4 cœurs ou plus |
| Système | Windows 10/11 (64 bits), macOS 12+, ou Linux 64 bits | — |
| Internet | oui : téléchargement des images Docker, des données (API de Chicago) | — |

Le projet fonctionne sur processeur Intel/AMD et sur Apple Silicon (M1/M2/M3) : toutes les images utilisées existent
pour les deux.

On installe seulement **deux logiciels** : **Git** (pour récupérer le code) et **Docker** (qui fait tourner tout le
reste : Python, Java, Spark, Airflow, la base de données, le stockage). Rien d'autre à installer à la main.

---

## 1. Installer Git

**Windows** : télécharger « Git for Windows » sur https://git-scm.com/download/win, lancer l'installateur, garder les
options par défaut. Ensuite, utiliser le terminal **Git Bash** (ou PowerShell).

**macOS** : ouvrir le Terminal et taper `git --version`. Si Git n'est pas là, macOS propose d'installer les
« outils de ligne de commande » : accepter. (Ou, avec Homebrew : `brew install git`.)

**Linux (Debian/Ubuntu)** :
```bash
sudo apt-get update && sudo apt-get install -y git
```

Vérifier : `git --version` doit afficher une version.

---

## 2. Installer Docker

### Windows
1. Activer WSL 2 (le Linux intégré à Windows) : ouvrir **PowerShell en administrateur** et taper `wsl --install`,
   puis **redémarrer**.
2. Télécharger **Docker Desktop** sur https://www.docker.com/products/docker-desktop/ et l'installer
   (laisser cochée l'option « Use WSL 2 »).
3. Lancer Docker Desktop et attendre que la baleine en bas à gauche soit verte (« Engine running »).
4. **Mémoire** : Docker Desktop → *Settings* → *Resources* : au moins **5 Go** de mémoire (avec WSL 2, si le réglage
   n'apparaît pas, créer le fichier `%UserProfile%\.wslconfig` contenant
   `[wsl2]` puis `memory=6GB` sur la ligne suivante, et redémarrer Docker).

### macOS
1. Télécharger **Docker Desktop** sur https://www.docker.com/products/docker-desktop/ : choisir *Apple Silicon* (puces
   M1/M2/M3) ou *Intel* selon la Mac (menu  → À propos de ce Mac).
2. Glisser Docker dans Applications, le lancer, accepter les autorisations.
3. **Mémoire** : Docker Desktop → *Settings* → *Resources* → *Memory* : au moins **5 Go**.

### Linux (Debian/Ubuntu)
```bash
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-v2   # sur Debian 13 : docker.io docker-compose
sudo usermod -aG docker $USER                         # pour utiliser docker sans sudo
# se déconnecter puis se reconnecter (ou redémarrer) pour que ce groupe compte
```
(Sur Linux, Docker utilise directement la mémoire de la machine : rien à régler.)

### Vérifier Docker (tous systèmes)
```bash
docker --version
docker compose version
docker run --rm hello-world
```
Le dernier doit afficher « Hello from Docker! ». Si `docker compose` n'existe pas mais `docker-compose` oui, utiliser
`docker-compose` partout à la place de `docker compose`.

---

## 3. Récupérer le projet

```bash
git clone https://github.com/omarajraoui/artefact-taxi.git
cd artefact-taxi
```

Le dépôt est public : pas besoin de compte GitHub pour le cloner. (S'il était privé : `gh auth login` ou un token,
voir `GUIDE.md`, section Git.)

---

## 4. Préparer la configuration

```bash
cp .env.example .env         # Windows PowerShell : copy .env.example .env
```

Le fichier `.env` contient tous les réglages. Les valeurs par défaut marchent. Les plus utiles :

| Variable | Défaut | À quoi ça sert |
|---|---|---|
| `TRIPS_START`, `TRIPS_END` | `2023-01-01`, `2023-04-01` | la période téléchargée (1,45 million de trajets) |
| `MAX_ROWS_PER_MONTH` | vide | mettre `20000` pour un essai rapide (≈ 2 min au lieu de ≈ 10) |
| `SPARK_DRIVER_MEMORY` | `2g` | mémoire de Spark ; `1500m` sur une petite machine |
| `SCHEDULER_MEM_LIMIT` | `3g` | plafond du conteneur où tourne Spark (garder ≥ mémoire Spark + 1 Go) |
| `MAX_REJECT_RATE` | `0.2` | au-delà de 20 % de lignes invalides, le pipeline s'arrête |
| `SODA_APP_TOKEN` | vide | optionnel : un jeton gratuit de l'API de Chicago pour éviter les limitations |

---

## 5. Lancer

```bash
docker compose up -d --build
```

Ce qui se passe (voir `PAS-A-PAS.md` pour le détail) :
1. **Construction des images** (5 à 15 min la première fois, quelques secondes ensuite) ;
2. **Démarrage des services** dans l'ordre : stockage → bucket → base Airflow → Airflow → API ;
3. **Le pipeline démarre tout seul** (≈ 10 min pour le trimestre complet).

Suivre l'état :
```bash
docker compose ps                          # chaque service : running / exited, healthy
docker compose logs -f airflow-scheduler   # le journal en direct (Ctrl+C pour quitter)
```

---

## 6. Vérifier les résultats

| Où | Adresse | Ce qu'on y voit |
|---|---|---|
| Airflow | http://localhost:8080 (`admin` / `admin`) | DAG `taxi_pipeline`, vue *Graph* : toutes les tâches vertes |
| Datalake | http://localhost:8888/buckets/datalake/ | les dossiers `bronze/`, `silver/`, `gold/` et leurs fichiers |
| Tableau de bord | http://localhost:8000 | revenus par jour, durées, top 10 des zones, taux de rejet |
| API | http://localhost:8000/docs | tous les points d'accès, essayables dans le navigateur |

Exemple de requête SQL sur le gold :
```bash
curl -s localhost:8000/sql -H 'content-type: application/json' \
  -d '{"query": "select pickup_zone, trips from top_pickup_zones order by trips desc limit 5"}'
```
(Windows PowerShell : utiliser la page http://localhost:8000/docs → `POST /sql` → *Try it out*.)

Résultat attendu avec la période par défaut : 1 446 439 lignes reçues, ≈ 2 % rejetées, 90 jours dans `daily_kpis`.

---

## 7. Relancer, arrêter, nettoyer

```bash
docker compose exec airflow-scheduler airflow dags trigger taxi_pipeline   # relancer le pipeline
docker compose down        # arrêter (les données restent)
docker compose up -d       # redémarrer (sans reconstruire)
docker compose down -v     # arrêter ET tout effacer (datalake + historique Airflow)
```

Avec `make` (Linux/macOS) : `make up`, `make trigger`, `make status`, `make logs`, `make test`, `make down`,
`make clean`.

**Lancer les tests** : `make test`, ou sans make :
```bash
docker compose run --rm --no-deps -v "$PWD/tests:/opt/airflow/tests" airflow-scheduler \
  bash -c "pip install -q pytest && python -m pytest -q /opt/airflow/tests"
```

---

## 8. Problèmes fréquents

| Symptôme | Cause | Solution |
|---|---|---|
| `Cannot connect to the Docker daemon` | Docker n'est pas lancé | ouvrir Docker Desktop (Windows/Mac) ; `sudo systemctl start docker` (Linux) |
| `permission denied ... docker.sock` (Linux) | utilisateur hors du groupe `docker` | `sudo usermod -aG docker $USER` puis se reconnecter, ou préfixer par `sudo` |
| `port is already allocated` | un autre logiciel utilise 8080, 8000, 8333 ou 8888 | fermer ce logiciel, ou changer le nombre **de gauche** dans `ports:` du `docker-compose.yml` |
| une tâche Spark échoue, `exit code 137` | plus assez de mémoire | baisser `SPARK_DRIVER_MEMORY`, augmenter `SCHEDULER_MEM_LIMIT` ou la mémoire de Docker Desktop |
| `ingest_month` reste orange (*up_for_retry*) | l'API de Chicago limite le débit | attendre (relances automatiques) ou mettre `SODA_APP_TOKEN` |
| le build échoue sur `pip install` | réseau coupé ou proxy d'entreprise | relancer `docker compose build` ; derrière un proxy, configurer celui de Docker Desktop |
| Windows : scripts qui échouent avec `\r` | fins de ligne Windows | avant de cloner : `git config --global core.autocrlf input`, puis recloner |
| le tableau de bord dit « not built yet » | le pipeline n'a pas fini | attendre la tâche `silver_to_gold` dans Airflow |
