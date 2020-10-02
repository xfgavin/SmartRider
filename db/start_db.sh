#!/usr/bin/env bash
docker run --name=postgis -d -e POSTGRES_USER=$PGUSER -e POSTGRES_PASS=$PGPWD -e POSTGRES_DBNAME=$PGDB -e ALLOW_IP_RANGE=10.0.0.0/24 -p 5432:5432 -v smartrider:/var/lib/postgresql -v `pwd`:/mnt --restart=always kartoza/postgis
