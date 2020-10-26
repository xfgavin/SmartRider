CREATE OR REPLACE PROCEDURE GEO2ZONE()
LANGUAGE plpgsql
AS $$
DECLARE
	REC RECORD;
	CUR CURSOR FOR SELECT ID FROM TRIPDATA_GEO;
BEGIN
	OPEN CUR;
	LOOP
		FETCH CUR INTO REC;
		EXIT WHEN NOT FOUND;
		RAISE NOTICE 'WORKING ON RECORD: %',REC.ID;
        INSERT INTO TRIPDATA(pickup_datetime,
	dropoff_datetime,
	PULocationID,
	DOLocationID,
	passenger_count,
	trip_distance,
	fare_amount,
	pickup_date,
	pickup_year,
	pickup_month,
	pickup_day,
	pickup_hour,
	duration,
	speed,
	traffic,
	rate,
	isholiday,
	isweekend,
        sourceID)
          SELECT trip.pickup_datetime,
            trip.dropoff_datetime,
            zone1.locationid as PULocationID,
            zone2.locationid as DOLocationID,
            trip.passenger_count,
            trip.trip_distance,
            trip.fare_amount,
            trip.pickup_date,
            trip.pickup_year,
            trip.pickup_month,
            trip.pickup_day,
            trip.pickup_hour,
            trip.duration,
            trip.speed,
            trip.traffic,
            trip.rate,
            trip.isholiday,
            trip.isweekend,
            trip.sourceID
          FROM
                tripdata_geo AS trip
            INNER JOIN taxi_zones AS zone1 
            ON st_within(ST_SetSRID( ST_Point(trip.pickup_longitude,trip.pickup_latitude ), 4326),zone1.geom )
            INNER JOIN taxi_zones as zone2
            ON st_within(ST_SetSRID( ST_Point(trip.dropoff_longitude,trip.dropoff_latitude ), 4326),zone2.geom )
          WHERE ID=REC.ID;
        DELETE FROM TRIPDATA_GEO 
        WHERE CURRENT OF CUR;
	END LOOP;
	CLOSE CUR;
END; $$
