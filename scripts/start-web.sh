#!/usr/bin/env bash

source ./common.sh
if ! [[ $REPLICAS =~ $IS_NUMBER_CHECK_REGEX ]]; then
  echo "Replicas was not an integer number;"
fi

export DATABASE_HOST=${DOCKER_HOST_IP}
export RABBIT_HOST=${DOCKER_HOST_IP}
echo "starting web interface"
$DOCKER_COMPOSE_CMD -f $DOCKER_COMPOSE_FILE up web -d
echo "docker compose -f $DOCKER_COMPOSE_FILE logs web -f"
