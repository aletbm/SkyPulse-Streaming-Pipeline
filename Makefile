ifneq (,$(wildcard .env))
    include .env
    export
endif

.PHONY: install lint \
        deploy \
        producers consumers jobs streaming \
        terraform-deploy terraform-destroy

# --- Paths ---
JOB_MANAGER    = deploy-jobmanager-1
REDPANDA_SERVICE 	   = deploy-redpanda-1
PATH_PRODUCERS = src/producers
PATH_CONSUMERS = src/consumers
PATH_JOBS      = src/jobs
PATH_DEPLOY    = deploy
PATH_INFRA     = infra
PATH_SCRIPTS   = scripts

# --- Dev ---
install:
	uv sync --all-groups

lint:
	uv run pre-commit autoupdate
	cmd /C "set PYTHONIOENCODING=utf-8 && uv run pre-commit run --all-files"

# --- Infrastructure ---
deploy:
	docker compose -f $(PATH_DEPLOY)/docker-compose.yml build redpanda jobmanager taskmanager
	docker compose -f $(PATH_DEPLOY)/docker-compose.yml up redpanda jobmanager taskmanager

deploy-destroy:
	docker compose -f $(PATH_DEPLOY)/docker-compose.yml down redpanda jobmanager taskmanager

postgres:
	docker compose -f $(PATH_DEPLOY)/docker-compose.yml build postgres
	docker compose -f $(PATH_DEPLOY)/docker-compose.yml up postgres

postgres-destroy:
	docker compose -f $(PATH_DEPLOY)/docker-compose.yml down postgres

run-pipeline:
	scripts\bruin\run_bruin.bat

# --- Streaming layer ---
clean-topics:
	docker exec $(REDPANDA_SERVICE) rpk topic delete $(TOPIC_SEISMIC)
	docker exec $(REDPANDA_SERVICE) rpk topic delete $(TOPIC_FLIGHTS)
	docker exec $(REDPANDA_SERVICE) rpk topic delete $(TOPIC_WEATHER)
	docker exec $(REDPANDA_SERVICE) rpk topic create $(TOPIC_SEISMIC)
	docker exec $(REDPANDA_SERVICE) rpk topic create $(TOPIC_FLIGHTS)
	docker exec $(REDPANDA_SERVICE) rpk topic create $(TOPIC_WEATHER)

producers:
	cmd /C "start "SP-SeismicProducer" uv run python $(PATH_PRODUCERS)/seismic_producer.py"
	cmd /C "start "SP-FlightProducer" uv run python $(PATH_PRODUCERS)/flight_producer.py"
	cmd /C "start "SP-WeatherProducer" uv run python $(PATH_PRODUCERS)/weather_producer.py"

consumers:
	cmd /C "start "SP-SeismicConsumer" uv run python $(PATH_CONSUMERS)/seismic_consumer.py"
	cmd /C "start "SP-FlightConsumer" uv run python $(PATH_CONSUMERS)/flight_consumer.py"
	cmd /C "start "SP-WeatherConsumer" uv run python $(PATH_CONSUMERS)/weather_consumer.py"

#http://localhost:8081
jobs:
	cmd /C "start "SP-SeismicTumblingJob" docker exec -it $(JOB_MANAGER) ./bin/flink run -py /opt/$(PATH_JOBS)/seismic_tumbling.py --pyFiles /opt/src"
	cmd /C "start "SP-FlightTumblingJob" docker exec -it $(JOB_MANAGER) ./bin/flink run -py /opt/$(PATH_JOBS)/flight_tumbling.py --pyFiles /opt/src"
	cmd /C "start "SP-WeatherTumblingJob" docker exec -it $(JOB_MANAGER) ./bin/flink run -py /opt/$(PATH_JOBS)/weather_tumbling.py --pyFiles /opt/src"
	cmd /C "start "SP-FlightContextTumblingJob" docker exec -it $(JOB_MANAGER) ./bin/flink run -py /opt/$(PATH_JOBS)/flight_context_tumbling.py --pyFiles /opt/src"

# Start everything:
wait-topics:
	uv run python $(PATH_SCRIPTS)/wait_topics.py

streaming:
	$(MAKE) producers
	$(MAKE) consumers
	$(MAKE) wait-topics
	$(MAKE) jobs

# --- Terraform ---
terraform-deploy:
	terraform -chdir=$(PATH_INFRA) init
	terraform -chdir=$(PATH_INFRA) validate
	terraform -chdir=$(PATH_INFRA) plan -out=tfplan
	terraform -chdir=$(PATH_INFRA) apply "tfplan"

terraform-destroy:
	terraform -chdir=$(PATH_INFRA) destroy -auto-approve
