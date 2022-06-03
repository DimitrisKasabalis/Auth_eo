#!/usr/bin/env bash
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
DOCKER_CMD="$( which docker || echo >&2 "Docker is required but it's not installed. Aborting" )"
if [ -z "$DOCKER_CMD" ]; then exit; fi
DOCKER_COMPOSE_CMD="${DOCKER_CMD} compose"
PROJ_FOLDER="${SCRIPT_DIR}/.."
DOCKER_COMPOSE_FILE="${PROJ_FOLDER}/docker-compose.yml"
if [ ! -f "$DOCKER_COMPOSE_FILE" ]; then
  echo "Docker compose file was not found"
fi

$DOCKER_COMPOSE_CMD -f $DOCKER_COMPOSE_FILE up -d rabbit db
$DOCKER_COMPOSE_CMD -f $DOCKER_COMPOSE_FILE logs -f
