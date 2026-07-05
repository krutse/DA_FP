
FROM python:3.14.4-slim

# install cron, dos2unix
RUN apt-get update && apt-get install -y --no-install-recommends \
    cron \
    dos2unix \
    && rm -rf /var/lib/apt/lists/*

# set schedule
COPY crontab /etc/cron.d/python-cron

RUN dos2unix /etc/cron.d/python-cron \
    && chmod 0644 /etc/cron.d/python-cron \
    && crontab /etc/cron.d/python-cron

WORKDIR /app 

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN touch /var/log/cron.log

RUN dos2unix /app/entrypoint.sh  \
    && chmod +x /app/entrypoint.sh

