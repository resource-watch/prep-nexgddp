#!/bin/bash
set -e

case "$1" in
    develop)
        echo "Running Development Server"
	      echo -e "$GCLOUD_STORAGE" | base64 -d > storage.json
        exec python main.py
        ;;
    test)
        echo "Test"
        ;;
    start)
        echo "Running Start"
	      echo -e "$GCLOUD_STORAGE" | base64 -d > storage.json
        exec gunicorn -c gunicorn.py nexgddp:app
        ;;
    *)
        exec "$@"
esac
