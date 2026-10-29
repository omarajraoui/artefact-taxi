.PHONY: up down logs trigger status test clean

up:            ## build and start everything (the DAG then runs once by itself)
	@test -f .env || cp .env.example .env
	docker compose up -d --build

down:          ## stop (keeps the data volumes)
	docker compose down

clean:         ## stop and delete every volume (datalake included)
	docker compose down -v

logs:
	docker compose logs -f airflow-scheduler

trigger:       ## run the pipeline again
	docker compose exec airflow-scheduler airflow dags trigger taxi_pipeline

status:
	docker compose exec airflow-scheduler airflow dags list-runs -d taxi_pipeline

test:          ## unit tests (Spark logic) inside the Airflow image
	docker compose run --rm --no-deps -e PYTHONPATH=/opt/airflow -v $(PWD)/tests:/opt/airflow/tests airflow-scheduler \
	  bash -c "pip install -q pytest && python -m pytest -q /opt/airflow/tests"
