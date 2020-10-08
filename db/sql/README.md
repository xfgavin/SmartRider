This folder contains:
1. scripts to set up postgis database
2. shapefiles of NYC taxi zone, LION, and census 2010 20C.
3. SQL scripts for various purposes.

Please modify them according to your need.

Shell scripts:
1. setup_db.sh: this script helps to:
   1. setup database schema.
   2. load shape files.
   3. create necessary tables.
   
2. start_db.sh: lauch postgis docker container

SQL scripts:
1. add_newark_airport.sql: add newark airport geometry information to census track 2010 20C
2. create_mapping.sql: create mapping between lion and taxi zone, as well as between census track and taxi zone.
3. create_tripdata.sql: create taxi trip data table.
4. refine_lion.sql: change data type of segmentid from text to integer in the lion table.

5. calc_rate.sql: calculate taxi rates based on:
   1. whether it was holiday or not
   2. whether it was weekend or not
   3. taxi zone
   4. pick up month
   5. pick hour
   6. traffic situation
   7. COVID or not
   
   This script should be run whenever new data comes in.
   
