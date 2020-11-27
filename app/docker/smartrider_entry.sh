#!/usr/bin/env bash
[ -z "${PGPWD}" ] && PGPWD=smartrider
[ -z "${PGDB}" ] && PGDB=smartrider
[ -z "${PGUSER}" ] && PGUSER=smartrider
[ -z "${PGHOST}" ] && PGHOST=localhost
echo $PGHOST
SCRIPTROOT=$( cd $(dirname $0) ; pwd)
$SCRIPTROOT/smartrider_app.py
