#!/usr/bin/env bash
set -Eeo pipefail
CONCURRENCY=${WORKERS_PER_INSTANCE:-1}


if [[ -n "$1" ]]; then
  if [ "$1" = "loaddata" ]; then
    echo "loaddata:" "${@:2}"
    python /src/manage.py loaddata "${@:2}"
    exit
  # if we start with a params assume we want to run that
  elif [ "$1" = "migrate" ]; then
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
    celery -A mproj.celery:app worker -l INFO --concurrency="${CONCURRENCY}"
    exit
  elif [ "$1" = 'worker-download' ]; then
    echo "!!!Starting Worker-download!!!"
    cd /src
    celery --app mproj.celery:app \
            worker \
            -l INFO \
            --concurrency="${CONCURRENCY}" \
            --queues download
    exit
  elif [ "$1" = 'worker-crawler' ]; then
    echo "!!!Starting worker-crawler!!!"
    cd /src
    celery --app mproj.celery:app \
            worker \
            -l INFO \
            --concurrency="${CONCURRENCY}" \
            --queues crawl
    exit
  elif [ "$1" = 'worker-process' ]; then
    echo "!!!Starting Worker-process!!!"
    cd /src
    celery --app mproj.celery:app \
            worker \
            -l INFO \
            --concurrency="${CONCURRENCY}" \
            --queues process
    exit
  elif [ "$1" = "beat" ]; then
    echo "Starting Beat"
    cd /src
    celery -A mproj.celery:app beat -l DEBUG --pidfile="$(mktemp)".pid -s "$(mktemp).db"
    exit
#  elif [ "$1" = 'web' ]; then
#      echo "Starting Web"
#      python /src/manage.py runserver 0.0.0.0:8000
  fi
fi

echo "Running command"
exec "$@"
