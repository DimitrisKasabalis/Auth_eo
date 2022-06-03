#!/usr/bin/env bash

source ./common.sh
REPLICAS="${1:-2}"
if ! [[ $REPLICAS =~ $IS_NUMBER_CHECK_REGEX ]]; then
  echo "Replicas was not an integer number;"
fi

export DATABASE_HOST=${DOCKER_HOST_IP}
export RABBIT_HOST=${DOCKER_HOST_IP}
echo "starting download workers"
$DOCKER_COMPOSE_CMD -f $DOCKER_COMPOSE_FILE up --scale worker-download=${REPLICAS} worker-download
