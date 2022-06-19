SHELL := /bin/bash
PWD := $(shell pwd)

GIT_REMOTE = github.com/7574-sistemas-distribuidos/docker-compose-init

docker-image:
	docker build -f Dockerfile -t "rust_process:latest" .
.PHONY: docker-image

docker-compose-up: docker-image
	docker-compose --env-file .env -f docker-compose-dev.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker-compose -f docker-compose-dev.yaml stop -t 5 client
	docker-compose -f docker-compose-dev.yaml stop -t 5 server
	docker-compose -f docker-compose-dev.yaml stop -t 5 worker_map
	docker-compose -f docker-compose-dev.yaml stop -t 5 worker_filter_students
	docker-compose -f docker-compose-dev.yaml stop -t 5 worker_filter_score
	docker-compose -f docker-compose-dev.yaml stop -t 5 worker_avg
	docker-compose -f docker-compose-dev.yaml stop -t 5 worker_join
	docker-compose -f docker-compose-dev.yaml stop -t 5 worker_group_by
	docker-compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker-compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs
