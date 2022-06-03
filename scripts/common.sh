#!/usr/bin/env bash
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
DOCKER_CMD="$( which docker || echo >&2 "Docker is required but it's not installed. Aborting" )"
if [ -z "$DOCKER_CMD" ]; then exit; fi
# define DOCKER_HOST_IP if not found
if [ -z  "$DOCKER_HOST_IP" ]; then
  DOCKER_HOST_IP=$( docker network inspect bridge -f '{{range .IPAM.Config}}{{.Gateway}}{{end}}' )
fi
echo "using $DOCKER_HOST_IP as DOCKER_HOST_IP (the ip to reach the host from inside a container)"
DOCKER_COMPOSE_CMD="${DOCKER_CMD} compose"
PROJ_FOLDER="${SCRIPT_DIR}/.."
DOCKER_COMPOSE_FILE="${PROJ_FOLDER}/docker-compose.yml"
IS_NUMBER_CHECK_REGEX='^[0-9]+$'
if [ ! -f "$DOCKER_COMPOSE_FILE" ]; then
  echo "Docker compose file was not found"
fi
