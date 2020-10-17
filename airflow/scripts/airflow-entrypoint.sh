#!/usr/bin/env bash
airflow initdb
airflow upgradedb
airflow scheduler &
airflow webserver
