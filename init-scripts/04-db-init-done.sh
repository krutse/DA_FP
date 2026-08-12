#!/bin/bash

echo "=== Finish script ==="

mkdir -p /var/lib/postgresql/data
# mkdir -p "$PGDATA"
# z_done.sh — запишется в самом конце
touch /var/lib/postgresql/data/init_done # /tmp/init_done
# touch "$PGDATA/init_done"
