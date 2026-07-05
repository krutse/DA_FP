#!/bin/sh

# keep env vars except (BASH, PATH, etc.)
env | tr -d '\r' | grep -v '^[[:space:]]*$' | grep -v -E '^(HOME|PWD|PATH|SHLVL|_)=' > /etc/environment

echo "[$(date)] Wait for PostgreSQL run at host postgres:5432..."

# until su -s /bin/sh -c "cat < /dev/null > /dev/tcp/postgres/5432" 2>dev/null; do
#  echo "[$(date) DB unavailable, wait for 1 sec...]"
#  sleep 1
# done

/usr/local/bin/python -c "
import socket, time, os
host = os.getenv('DB_HOST', 'postgres')
port = int(os.getenv('DB_PORT', 5432))
while True:
    try:
        with socket.create_connection((host, port), timeout=2):
            print('DB answer successfully', flush=True)
            break
    except OSError:
        print('DB is not ready, wait for 10 sec...', flush=True)
        time.sleep(10)
"

echo "[$(date)] script ran at conteiner start."
/usr/local/bin/python /app/main.py

echo "[$(date)]  Cron start."
cron

# keep container running
touch /var/log/cron.log
tail -f /var/log/cron.log


