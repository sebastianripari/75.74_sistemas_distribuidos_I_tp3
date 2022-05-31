SHELL := /bin/bash
PWD := $(shell pwd)

GIT_REMOTE = github.com/7574-sistemas-distribuidos/docker-compose-init

default: build

all:

deps:
	go mod tidy
	go mod vendor

build: deps
	GOOS=linux go build -o bin/client github.com/7574-sistemas-distribuidos/docker-compose-init/client
.PHONY: build

docker-image:
	docker build -f ./server/Dockerfile -t "server:latest" .
	docker build -f ./worker_initial_state/Dockerfile -t "worker_initial_state:latest" .
	docker build -f ./worker_avg/Dockerfile -t "worker_avg:latest" .
	docker build -f ./worker_map/Dockerfile -t "worker_map:latest" .
	docker build -f ./worker_filter_score/Dockerfile -t "worker_filter_score:latest" .
	docker build -f ./worker_filter_students/Dockerfile -t "worker_filter_students:latest" .
	docker build -f ./worker_join/Dockerfile -t "worker_join:latest" .
	docker build -f ./sender/Dockerfile -t "sender:latest" .
	
	# Execute this command from time to time to clean up intermediate stages generated 
	# during client build (your hard drive will like this :) ). Don't left uncommented if you 
	# want to avoid rebuilding client image every time the docker-compose-up command 
	# is executed, even when client code has not changed
	# docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`
.PHONY: docker-image

docker-compose-up: docker-image
	docker-compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker-compose -f docker-compose-dev.yaml stop -t 1
	docker-compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker-compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs
