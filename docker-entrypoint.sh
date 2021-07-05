#!/usr/bin/env bash
set -Eeo pipefail
CONCURRENCY=${WORKERS_PER_INSTANCE:-1}

#. /opt/conda/etc/profile.d/conda.sh
#conda activate base

if [[ -n "$1" ]]; then
  #  echo "dsf"
  # if we start with a params assume we want to run that
  if [ "$1" = "migrate" ]; then
    echo "migrate"
    python /src/manage.py migrate
    exit
  elif [ "$1" = 'shell' ]; then
    python /src/manage.py shell
    exit
  elif [ "$1" = 'scrape' ]; then
    # scrap -as-task <name-of-spider>
    python /src/manage.py scrape "${@:2}"
    exit
  elif [ "$1" = 'download-file' ]; then
    python /src/manage.py download_file "${@:2}"
    exit
  elif [ "$1" = 'worker' ]; then
    echo "!!!Starting Worker!!!"
    cd /src
    celery -A mproj.celery:app worker -l info --concurrency="${CONCURRENCY}"
    exit
  elif [ "$MODE" = "beat" ]; then
    echo "Starting Beat"
    cd /src
    celery -A mproj.celery:app beat -l info -s /celerybeat-schedule.d --pidfile="$(mktemp)".pid
    exit
  fi

fi

echo "Running command"
exec "$@"
