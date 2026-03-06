.PHONY: run run-db stop delete image package clean build run-sonar avro kube-apply-bash kube-delete-bash kube-apply-ps kube-delete-ps prune run-cassandra run-confluent run-app stop-cassandra stop-confluent stop-app stop-all status logs sonar test integrationTest package
# Load environment variables from .env file if it exists
-include .env

# OS Detection: Use gradlew.bat on Windows, ./gradlew on Unix/Linux/Mac
ifeq ($(OS),Windows_NT)
    GRADLEW := gradlew.bat
else
    GRADLEW := ./gradlew
endif

PROJECT ?= data-service-student-enrollment

IMAGE ?= flipfoundry.io/$(PROJECT)
BUILD ?= latest

DOCKER_COMPOSE ?= docker-compose
COMPOSE ?= $(DOCKER_COMPOSE) -f docker-compose.yml

# CASSANDRA TARGETS
run-cassandra:
	@echo "Starting Cassandra..."
	cd cassandra && $(DOCKER_COMPOSE) up -d
	@echo "Cassandra is starting. Waiting for health check..."
	@timeout /t 15 /nobreak
	@echo "Cassandra initialized and ready"

stop-cassandra:
	@echo "Stopping Cassandra..."
	cd cassandra && $(DOCKER_COMPOSE) stop
	cd cassandra && $(DOCKER_COMPOSE) rm -f

logs-cassandra:
	cd cassandra && $(DOCKER_COMPOSE) logs -f cass

# CONFLUENT/KAFKA TARGETS
run-confluent:
	@echo "Starting Confluent (Kafka, Schema Registry, Control Center)..."
	cd confluent && $(DOCKER_COMPOSE) up -d
	@echo "Waiting for Kafka to be ready..."
	@timeout /t 10 /nobreak
	@echo "Confluent stack is up. Control Center: http://localhost:9021"

stop-confluent:
	@echo "Stopping Confluent..."
	cd confluent && $(DOCKER_COMPOSE) stop
	cd confluent && $(DOCKER_COMPOSE) rm -f

logs-confluent:
	cd confluent && $(DOCKER_COMPOSE) logs -f

# APPLICATION TARGETS
run-app: build
	@echo "Starting Data Service Application locally..."
	@echo "Application starting. API: http://localhost:8700"
	gradle bootRun

stop-app:
	@echo "Stopping Application..." 
	@taskkill /F /IM java.exe 2>nul || echo "Java process not running"

logs-app:
	$(COMPOSE) logs -f app

# STARTUP SEQUENCE
# Run these commands in order in separate terminals:
#   Terminal 1: make run-cassandra
#   Terminal 2: make run-confluent
#   Terminal 3: make generate-data
#   Terminal 4: make run-app
#
# This avoids race conditions where the app starts before topics are created.

stop-all: stop-app stop-confluent stop-cassandra
	@echo "All services stopped!"

status:
	@echo "=== Cassandra ==="
	cd cassandra && $(DOCKER_COMPOSE) ps || echo "Not running"
	@echo ""
	@echo "=== Confluent ==="
	cd confluent && $(DOCKER_COMPOSE) ps || echo "Not running"
	@echo ""
	@echo "=== Application ==="
	$(COMPOSE) ps || echo "Not running"

logs: logs-cassandra logs-confluent logs-app

# BUILD TARGETS
build:
	$(GRADLEW) clean
	$(GRADLEW) build

test: build
	$(GRADLEW) test integrationTest

integrationTest: build
	$(GRADLEW) integrationTest --rerun-tasks

clean:
	$(GRADLEW) clean

# QUALITY & ANALYSIS TARGETS
sonar:
	$(GRADLEW) test integrationTest jacocoTestReport sonarqube

package:
	$(GRADLEW) build -x test

bootrun:
	$(GRADLEW) bootRun

image: build
	$(GRADLEW) jibDockerBuild

run:
	$(COMPOSE) up

stop:
	$(COMPOSE) stop
	$(COMPOSE) rm -f

delete:
	docker image rm $(IMAGE)

# DATA GENERATION TARGETS
install-datagen:
	cd data-generator && pip install -r requirements.txt

generate-data:
	@echo "Generating 50 student/enrollment pairs..."
	cd data-generator && python data_generator.py --records 50 --interval 1

generate-data-small:
	@echo "Generating 10 student/enrollment pairs (quick demo)..."
	cd data-generator && python data_generator.py --records 10 --interval 0.5

generate-data-large:
	@echo "Generating 200 student/enrollment pairs (large demo)..."
	cd data-generator && python data_generator.py --records 200 --interval 0.2

generate-students-only:
	@echo "Generating 50 students only (no enrollments)..."
	cd data-generator && python data_generator.py --records 50 --students-only

generate-data-mock:
	@echo "Mock run (no Kafka connection)..."
	cd data-generator && python data_generator.py --mock --records 25

# PRUNE

prune:
	docker system prune -f
	docker network prune -f
	docker volume prune -f
