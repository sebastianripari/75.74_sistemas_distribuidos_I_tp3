SHELL := /bin/bash
PWD := $(shell pwd)

GIT_REMOTE = github.com/7574-sistemas-distribuidos/docker-compose-init

docker-image:
	docker build -f ./server/Dockerfile -t "server:latest" .
	docker build -f ./worker_initial_state/Dockerfile -t "worker_initial_state:latest" .
	docker build -f ./worker_avg/Dockerfile -t "worker_avg:latest" .
	docker build -f ./worker_map/Dockerfile -t "worker_map:latest" .
	docker build -f ./worker_filter_score/Dockerfile -t "worker_filter_score:latest" .
	docker build -f ./worker_filter_students/Dockerfile -t "worker_filter_students:latest" .
	docker build -f ./worker_join/Dockerfile -t "worker_join:latest" .
	docker build -f ./worker_group_by/Dockerfile -t "worker_group_by:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
.PHONY: docker-image

docker-compose-up: docker-image
	docker-compose --env-file .env -f docker-compose-dev.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker-compose -f docker-compose-dev.yaml stop -t 5 client
	docker-compose -f docker-compose-dev.yaml stop -t 5 server
	docker-compose -f docker-compose-dev.yaml stop -t 5 worker_initial_state
	docker-compose -f docker-compose-dev.yaml stop -t 5 worker_avg 
	docker-compose -f docker-compose-dev.yaml stop -t 5 worker_map 
	docker-compose -f docker-compose-dev.yaml stop -t 5 worker_filter_score
	docker-compose -f docker-compose-dev.yaml stop -t 5 worker_filter_students
	docker-compose -f docker-compose-dev.yaml stop -t 5 worker_join  
	docker-compose -f docker-compose-dev.yaml stop -t 5 worker_group_by
	docker-compose -f docker-compose-dev.yaml stop -t 10 rabbitmq 

	docker-compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker-compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs
