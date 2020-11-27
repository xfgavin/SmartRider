#!/usr/bin/env bash
[ -z "${PGPWD}" ] && export PGPWD=smartrider
[ -z "${PGDB}" ] && export PGDB=smartrider
[ -z "${PGUSER}" ] && export PGUSER=smartrider
[ -z "${PGHOST}" ] && export PGHOST=localhost
echo $PGHOST
SCRIPTROOT=$( cd $(dirname $0) ; pwd)
$SCRIPTROOT/smartrider_app.py
