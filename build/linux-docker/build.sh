#!/usr/bin/env bash
if [ $? -ne 0 ];
then
    echo "The path /usr/bin/env does not exit."
    exit 1
fi

# Correspond to the build stage for the Batch-job pipeline file
if [ -d "../../src" ];
then
    cd ../../src
else
    echo "No src directory. Please create one or check the path. The Batch-job project should under the home directory."
    exit 1
fi

go build -o ../batch-service

if [ $? -ne 0 ];
then
    echo "GO build did not success. Please check whether you install the golang."
    exit 1
fi

# Correspond to the docker-build stage for the Batch-job pipeline file
if [ -z "$PLANETROVER_REGISTRY_USERNAME" ] || [ -z "$PLANETROVER_REGISTRY_PASSWORD" ];
then
    echo "Please set environment variables PLANETROVER_REGISTRY_USERNAME and PLANETROVER_REGISTRY_PASSWORD"
    exit 1
fi

DOCKER_PATH=..
DOCKERFILE=${DOCKER_PATH}/Dockerfile

if [ ! -f "$DOCKERFILE" ];
then
    echo "Dockerfile does not exist. Please check the path you are at."
    exit 1
fi

TAG=`date +%s`
TARGET='batch-job'
CI_REGISTRY=gitlab.planetrover.ca:5050
HTTP_PROXY=http://proxy.planetrover.ca:8888
NO_PROXY=localhost,127.0.0.0/8,10.254.0.0/16,192.168.0.0/16,::1,docker,gitlab.planetrover.ca

docker login -u "${PLANETROVER_REGISTRY_USERNAME}" -p "${PLANETROVER_REGISTRY_PASSWORD}" ${CI_REGISTRY}

if [ $? -ne 0 ];
then
    echo "Log in failed. Please check whether your username or passoword is correct."
    exit 1
fi

docker build --tag "${TARGET}:${TAG}" --build-arg CI_REGISTRY=${CI_REGISTRY} --build-arg HTTP_PROXY=${HTTP_PROXY} --build-arg NO_PROXY=${NO_PROXY}  --pull --network host -f ${DOCKERFILE} ${DOCKER_PATH}

if [ $? -ne 0 ];
then
    echo "Docker build failed."
    exit 1
fi

echo "Newly build images can be found locally: ${TARGET}:${TAG}"

exit 0


