#!/usr/bin/env bash
Usage(){
[ ${#Error} -gt 0 ] && echo -e "\nError: $Error\n"
cat <<EOF
++++++++++++++++++++++++++++++++++++++++++++++++++++
Set up postgis database for smartrider project
by xfgavin@gmail.com 09/30/2020 @RB
+++++++++++++++++++++++++++++++++++++++++++++++++++++
usage: `basename $0` options
OPTIONS:
   -a      Do all steps
   -i      Install shp2pgsql
   -p      prepare database
   -t      load NY Taxi zone shape file
   -c      load NY census tract shape file
   -l      load NY LION shape file
   -m      create mapping between LION and taxi zone, and between taxi zone and census tract, add airport
Example:
  `basename $0` -i /path/to/csv -c column1
EOF
[ ${#Error} -gt 0 ] && exit -1
exit 0
}
prepare_db(){
  docker exec postgis bash -c "PGPASSWORD=$PGPWD psql -d $PGDB -U $PGUSER -h $PGHOST -c \"create schema smartrider;\""
  docker exec postgis bash -c "PGPASSWORD=$PGPWD psql -d $PGDB -U $PGUSER -h $PGHOST -c \"CREATE EXTENSION postgis;\""
}
install_shp2pgsql(){
  docker exec postgis bash -c "apt-get update;apt-get -y install postgis;/etc/init.d/postgresql reload"
}
load_taxizone(){
  docker exec postgis bash -c "shp2pgsql -s 2263:4326 -I /mnt/shapefile/taxi_zones/taxi_zones.shp | PGPASSWORD=$PGPWD psql -d $PGDB -U $PGUSER -h $PGHOST"
  docker exec postgis bash -c "PGPASSWORD=$PGPWD psql -d $PGDB -U $PGUSER -h $PGHOST -c \"CREATE INDEX ON taxi_zones (locationid);\""
  docker exec postgis bash -c "PGPASSWORD=$PGPWD psql -d $PGDB -U $PGUSER -h $PGHOST -c \"VACUUM ANALYZE taxi_zones;\""
}
load_nyct(){
  docker exec postgis bash -c "shp2pgsql -s 2263:4326 -I /mnt/shapefile/nyct2010_20c/nyct2010.shp | PGPASSWORD=$PGPWD psql -d $PGDB -U $PGUSER -h $PGHOST"
  docker exec postgis bash -c "PGPASSWORD=$PGPWD psql -d $PGDB -U $PGUSER -h $PGHOST -c \"CREATE INDEX ON nyct2010 (ntacode);\""
  docker exec postgis bash -c "PGPASSWORD=$PGPWD psql -d $PGDB -U $PGUSER -h $PGHOST -c \"VACUUM ANALYZE nyct2010;\""
}
load_lion(){
  docker exec postgis bash -c "shp2pgsql -s 4326 -I /mnt/shapefile/nyclion_20c/LION.shp | PGPASSWORD=$PGPWD psql -d $PGDB -U $PGUSER -h $PGHOST"
  docker exec postgis bash -c "PGPASSWORD=$PGPWD psql -d $PGDB -U $PGUSER -h $PGHOST -f /mnt/sql/refine_lion.sql"
  docker exec postgis bash -c "PGPASSWORD=$PGPWD psql -d $PGDB -U $PGUSER -h $PGHOST -c \"CREATE INDEX ON lion (segmentid);\""
  docker exec postgis bash -c "PGPASSWORD=$PGPWD psql -d $PGDB -U $PGUSER -h $PGHOST -c \"VACUUM ANALYZE lion;\""
}

create_mapping(){
  docker exec postgis bash -c "PGPASSWORD=$PGPWD psql -d $PGDB -U $PGUSER -h $PGHOST -f /mnt/sql/create_mapping.sql"
  docker exec postgis bash -c "PGPASSWORD=$PGPWD psql -d $PGDB -U $PGUSER -h $PGHOST -f /mnt/sql/add_newark_airport.sql"
  docker exec postgis bash -c "PGPASSWORD=$PGPWD psql -d $PGDB -U $PGUSER -h $PGHOST -f /mnt/sql/create_taxi_fare.sql"
}
while getopts "aiptclm" OPTION
do
  case $OPTION in
    a)
      prepare_db
      install_shp2pgsql
      load_taxizone
      load_nyct
      load_lion
      create_mapping
      ;;
    i)
      install_shp2pgsql
      ;;
    p)
      prepare_db
      ;;
    t)
      load_taxizone
      ;;
    c)
      load_nyct
      ;;
    l)
      load_lion
      ;;
    m)
      create_mapping
      ;;
    ?)
      Usage
      exit
      ;;
  esac
done
