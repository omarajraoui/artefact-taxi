# Ce qui se passe, étape par étape, de `docker compose up` au tableau de bord

Les chiffres viennent du lancement complet du 29/10/2026 (T1 2023, VM 4 cœurs / 5,8 Go).

---

## Étape 1 — `docker compose up -d --build` : construire et démarrer

1. Docker **construit** les deux images du projet à partir de leurs Dockerfile (Airflow + Java + Spark + jars S3 ;
   API + DuckDB) et **télécharge** les images toutes faites (SeaweedFS, Postgres).
2. Docker Compose **démarre les services dans l'ordre** fixé par `depends_on` :

```
s3 (stockage) ──healthy──► bucket-init (crée le bucket « datalake », s'arrête)
postgres ──healthy──┐                    │
                    └────────────────────┴──► airflow-init (prépare la base Airflow, crée admin, s'arrête)
                                                    └──► airflow-webserver (port 8080) + airflow-scheduler
bucket-init ──► api (port 8000)
```

3. Le **scheduler** lit `dags/taxi_pipeline.py`. Le DAG est programmé `@once` et non suspendu : il **démarre seul**.

---

## Étape 2 — `plan_months` : découper la période

Lit `TRIPS_START=2023-01-01` et `TRIPS_END=2023-04-01` dans `.env` et renvoie trois fenêtres d'un mois :
`[2023-01-01, 2023-02-01)`, `[2023-02-01, 2023-03-01)`, `[2023-03-01, 2023-04-01)`.

---

## Étape 3 — `ingest_month` × 3 : la couche BRONZE

Airflow crée **une tâche par mois** (trois en parallèle). Chaque tâche :

1. regarde si le mois est déjà complet (fichier `_manifest.json` présent) → si oui, elle s'arrête (rien à refaire) ;
2. sinon, efface un éventuel mois incomplet, puis demande les pages à l'API, 50 000 lignes à la fois ;
3. dépose chaque page **telle quelle** dans le datalake ;
4. écrit le `_manifest.json` **en dernier** (requête, nombre de pages et de lignes, durée).

Résultat dans le stockage :
```
bronze/taxi_trips/month=2023-01/page=00001.csv … page=00011.csv + _manifest.json
bronze/taxi_trips/month=2023-02/…
bronze/taxi_trips/month=2023-03/page=00001.csv … page=00012.csv + _manifest.json   (585 216 lignes)
```
**1 446 439 lignes, 30 pages, 644 Mo de CSV, ≈ 3 minutes.**

En parallèle, `ingest_reference` copie `seeds/community_areas.csv` (les 77 zones) dans
`bronze/reference/community_areas/`.

---

## Étape 4 — `check_bronze` : un premier contrôle

Si un mois est revenu **vide** (API en panne, filtre faux), la tâche échoue et rien ne continue. Ici : 3 mois remplis.

---

## Étape 5 — `bronze_to_silver` (Spark) : la couche SILVER

Airflow lance `spark-submit pipeline/spark/bronze_to_silver.py` dans le conteneur du scheduler. Spark :

1. **lit** les 30 CSV (le dossier `month=…` devient une colonne) ;
2. **type** chaque colonne (texte → date, nombre) ;
3. **contrôle** chaque ligne avec 7 règles et note toutes celles qu'elle viole ;
4. **dédoublonne** les lignes valides par `trip_id` ;
5. **ajoute** date, heure, jour de la semaine, durée en minutes, vitesse, taux de pourboire ;
6. **écrit** :
   * `silver/taxi_trips/trip_date=2023-01-01/ … trip_date=2023-03-31/` : 90 dossiers de Parquet, **120 Mo**
     (contre 644 Mo de CSV : ≈ 5 fois plus léger) ;
   * `silver/taxi_trips_rejected/` : les lignes rejetées avec leurs raisons ;
   * `silver/_quality/latest.json` : le rapport qualité.

Rapport qualité réel :
```json
{"rows_in": 1446439, "rows_clean": 1416392, "rows_rejected": 30047, "reject_rate": 0.020773,
 "rejected_by_reason": {"bad_duration": 28767, "bad_amount": 1347, "end_before_start": 36, "bad_distance": 25},
 "passed": true}
```
2,08 % de rejets < 20 % : la **porte qualité** laisse passer. **≈ 3 min 30 s.**

---

## Étape 6 — `silver_to_gold` (Spark) : la couche GOLD

Spark lit le silver et la table des zones, calcule et écrit quatre tables :

| Table | Lignes | Exemple de contenu |
|---|---|---|
| `gold/daily_kpis/` | 90 (un par jour) | trajets, revenu, durée moyenne / médiane / p90 |
| `gold/top_pickup_zones/` | 78 (77 zones + « inconnue ») | 1. Near North Side 21,1 %, 2. O'Hare 16,9 %, 3. Loop 15,9 % |
| `gold/hourly_demand/` | 168 (7 jours × 24 h) | trajets par jour de la semaine et par heure |
| `gold/payment_mix/` | 7 | trajets et revenu par moyen de paiement |

**≈ 1 min 10 s.** Le DAG est vert.

---

## Étape 7 — l'exposition : API et tableau de bord

L'API (port 8000) n'a rien à charger : DuckDB lit les fichiers Parquet du gold **directement dans le stockage**.

* http://localhost:8000 → le tableau de bord : 1 416 392 trajets, **36,46 M$** de revenu, **19,4 min** de durée
  moyenne, graphiques par jour, top 10 des zones ;
* http://localhost:8000/docs → l'API complète (JSON, SQL, rapport qualité).

---

## Et quand on relance ?

* `make trigger` : les trois mois ont leur `_manifest.json` → **ingestion sautée en quelques secondes** ; Spark
  recalcule silver et gold à partir du bronze déjà là.
* `{"force_ingest": true}` : retélécharge les mois.
* Si une tâche Spark échoue, on la relance seule depuis Airflow (bouton *Clear*) : l'ingestion n'est pas refaite.
  C'est exactement ce qui s'est passé lors du premier lancement complet (voir `GUIDE.md`, section 4).
