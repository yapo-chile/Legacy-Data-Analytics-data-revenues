include scripts/commands/vars.mk

## Deletes all containers
docker-remove: docker-stop
	docker-compose rm -f

## Stops all containers
docker-stop:
	docker-compose stop

## Compiles all the services
docker-build: build
	docker-compose build --no-cache

## Compile and start the service using docker
compose-up: docker-build
	docker-compose up -d

## compose up and no build
compose-up-no-build:
	docker-compose up -d

start: compose-up

## Only run image
run: compose-up-no-build

## Executre job with not container
### deliver to would be sending the generated report to that email if
### executed locally
run-local:
	python app/main.py -deliver_to=data_team@adevinta.com

## Publishes container
docker-publish:
	@scripts/commands/docker-publish.sh

## Execute the service
remove:
	docker stop ${APPNAME}
	docker rm ${APPNAME}

## Compile and start the service
build:
	@scripts/commands/docker-build.sh

## Compile and start the service using docker
reboot: remove build
	docker run -d --name ${APPNAME}  -p ${SERVER_EXPOSED_PORT}:${SERVER_PORT} ${DOCKER_IMAGE}:${BUILD_TAG}

# Installs libraries locally
install:
	pip install -r app/requirements.txt

# Execute style tests
check-style:
	@scripts/commands/check-style.sh

# deploys job to Rundeck
rundeck-deploy:
	@scripts/commands/rundeck-deploy.sh