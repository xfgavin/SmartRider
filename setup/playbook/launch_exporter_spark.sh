#!/usr/bin/env bash
cd /opt/www/status.dtrace.net
docker-compose -p status.dtrace.net up -d >/dev/null 2>&1
