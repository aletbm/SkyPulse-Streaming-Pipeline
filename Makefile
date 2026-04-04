ifneq (,$(wildcard .env))
    include .env
    export
endif

.PHONY: install lint \
        deploy \
        producers consumers jobs streaming \
        terraform-deploy terraform-destroy

# --- Paths ---
JOB_MANAGER    		= 	deploy-jobmanager-1
REDPANDA_SERVICE 	= 	deploy-redpanda-1
PATH_PRODUCERS 		= 	src/producers
PATH_CONSUMERS 		= 	src/consumers
PATH_JOBS      		= 	src/jobs
PATH_DEPLOY    		= 	deploy
PATH_INFRA     		= 	infra
PATH_SCRIPTS   		= 	scripts

install:
	uv sync --all-groups

lint:
	uv run pre-commit autoupdate
	cmd /C "set PYTHONIOENCODING=utf-8 && uv run pre-commit run --all-files"

deploy:
	docker compose -f $(PATH_DEPLOY)/docker-compose.yml build redpanda jobmanager taskmanager
	docker compose -f $(PATH_DEPLOY)/docker-compose.yml up redpanda jobmanager taskmanager

deploy-destroy:
	docker compose -f $(PATH_DEPLOY)/docker-compose.yml down redpanda jobmanager taskmanager

flink:
	docker compose -f $(PATH_DEPLOY)/docker-compose.flink.yml build
	docker compose -f $(PATH_DEPLOY)/docker-compose.flink.yml up

flink-destroy:
	docker compose -f $(PATH_DEPLOY)/docker-compose.flink.yml down

postgres:
	docker compose -f $(PATH_DEPLOY)/docker-compose.yml build postgres
	docker compose -f $(PATH_DEPLOY)/docker-compose.yml up postgres

postgres-destroy:
	docker compose -f $(PATH_DEPLOY)/docker-compose.yml down postgres

run-pipeline:
	scripts\bruin\run_bruin.bat

run-app:
	uv run streamlit run app/app.py

clean-topics:
	docker exec $(REDPANDA_SERVICE) rpk topic delete $(TOPIC_SEISMIC)
	docker exec $(REDPANDA_SERVICE) rpk topic delete $(TOPIC_FLIGHTS)
	docker exec $(REDPANDA_SERVICE) rpk topic delete $(TOPIC_WEATHER)
	docker exec $(REDPANDA_SERVICE) rpk topic create $(TOPIC_SEISMIC)
	docker exec $(REDPANDA_SERVICE) rpk topic create $(TOPIC_FLIGHTS)
	docker exec $(REDPANDA_SERVICE) rpk topic create $(TOPIC_WEATHER)

producers:
	docker build -f src/Dockerfile.producers -t aletbm/skypulse-producers:latest .
	docker push aletbm/skypulse-producers:latest
	docker run --env-file .env -it --rm aletbm/skypulse-producers:latest

consumers:
	docker build -f src/Dockerfile.consumers -t aletbm/skypulse-consumers:latest .
	docker push aletbm/skypulse-consumers:latest
	docker run --env-file .env -it --rm aletbm/skypulse-consumers:latest

#http://localhost:8081
jobs:
	docker build -f src/Dockerfile.jobs -t aletbm/skypulse-jobs:latest .
	docker push aletbm/skypulse-jobs:latest

infra-deploy:
	infra\setup.bat

infra-destroy:
	terraform -chdir=$(PATH_INFRA)/terraform destroy -auto-approve
