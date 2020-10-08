#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
import sys
import os
import holidays

def main(s3file):
    #This function will:
    # 1. Load data from s3
    # 2. Give raw data a good masssage
    # 3. save to database
    #s3file='yellow_tripdata_2020-04.csv'
    raw_df = read_clean_csv(s3file)
    tripdata_df = process_tripdata(raw_df)
    save2db(tripdata_df,'tripdata')
    
def read_clean_csv(s3file):
    #This function will:
    #  1. load raw NYC taxi trip data from S3
    #  2. unify column names
    #  3. clean up
    #  4. convert geo location to taxi zone id.
    input_filename = os.path.basename(s3file).lower()
    input_fileyear = input_filename.split('_')[2]
    input_fileyear = int(input_fileyear.split('-')[0])
    raw_df = spark.read.csv(s3file, header = True, inferSchema=True, multiLine=True, escape='"')
    if 'yellow' in input_filename:
        #Handel yellow taxi trip data
        if input_fileyear == 2009:
            raw_df = raw_df.withColumnRenamed("Trip_Pickup_DateTime", "pickup_datetime") \
                           .withColumnRenamed("Trip_Dropoff_DateTime", "dropoff_datetime") \
                           .withColumnRenamed("Passenger_Count", "passenger_count") \
                           .withColumnRenamed("Trip_Distance", "trip_distance") \
                           .withColumnRenamed("Tip_Amt", "tip_amount") \
                           .withColumnRenamed("Total_Amt", "total_amount")
        elif input_fileyear > 2014:
            raw_df = raw_df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
                           .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
    elif 'green' in input_filename:
        #Handel green taxi trip data
        if input_fileyear < 2016:
            raw_df = raw_df.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
                           .withColumnRenamed("Lpep_dropoff_datetime", "dropoff_datetime") \
                           .withColumnRenamed("Passenger_count", "passenger_count") \
                           .withColumnRenamed("Trip_distance", "trip_distance") \
                           .withColumnRenamed("Tip_amount", "tip_amount") \
                           .withColumnRenamed("Total_amount", "total_amount")
        elif input_fileyear>2015:
            raw_df = raw_df.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
                           .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")

    #Cleaning up data with invalid values for a given column.
    #minimal fare is based on https://www1.nyc.gov/site/tlc/passengers/taxi-fare.page
    raw_df = raw_df.filter((raw_df.pickup_datetime != "") & \
                           (raw_df.dropoff_datetime != "") & \
                           (raw_df.passenger_count >0) & \
                           (raw_df.trip_distance >0.5) & \
                           (raw_df.tip_amount >=0) & \
                           (raw_df.total_amount >=2.5))
    
    if "Start_Lat" in raw_df.schema.names:
        #Yellow_2009
        raw_df = raw_df.filter((raw_df.Start_Lat != 0) & (raw_df.Start_Lon != 0))
        raw_df['PULocationID'] = [geo2taxizoneid(Start_Lat,Start_Lon) for Start_Lat, Start_Lon in zip(raw_df['Start_Lat'], raw_df['Start_Lon'])]
        raw_df['DOLocationID'] = [geo2taxizoneid(End_Lat,End_Lon) for End_Lat, End_Lon in zip(raw_df['End_Lat'], raw_df['End_Lon'])]
    elif "pickup_longitude" in raw_df.schema.names:
        #Yellow_2010-2015
        raw_df = raw_df.filter((raw_df.pickup_latitude != 0) & (raw_df.pickup_longitude != 0))
        raw_df['PULocationID'] = [geo2taxizoneid(pickup_latitude,pickup_longitude) for pickup_latitude, pickup_longitude in zip(raw_df['pickup_latitude'], raw_df['pickup_longitude'])]
        raw_df['DOLocationID'] = [geo2taxizoneid(dropoff_latitude,dropoff_longitude) for dropoff_latitude, dropoff_longitude in zip(raw_df['dropoff_latitude'], raw_df['dropoff_longitude'])]
    elif "Pickup_longitude" in raw_df.schema.names:
        #Green_2013-2015
        raw_df = raw_df.filter((raw_df.Pickup_latitude != 0) & (raw_df.Pickup_longitude != 0))
        raw_df['PULocationID'] = [geo2taxizoneid(Pickup_latitude,Pickup_longitude) for Pickup_latitude, Pickup_longitude in zip(raw_df['Pickup_latitude'], raw_df['Pickup_longitude'])]
        raw_df['DOLocationID'] = [geo2taxizoneid(Dropoff_latitude,Dropoff_longitude) for Dropoff_latitude, Dropoff_longitude in zip(raw_df['Dropoff_latitude'], raw_df['Dropoff_longitude'])]
    
    #Only include valid PUlocationID.
    raw_df = raw_df.filter((raw_df.PULocationID >0))
    
    raw_df = raw_df.withColumn('pickup_datetime',to_timestamp(col('pickup_datetime')))\
                   .withColumn('dropoff_datetime', to_timestamp(col('dropoff_datetime')))
    #Reorganize data
    cols=['pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount', 'total_amount']
    return raw_df.select(cols)

def process_tripdata(raw_df):
    #This function uses spark.sql to compute some fields, including pickup_date, pickup_year, pickup_month, pickup_day, pickup_hour, duration, rate, isweekend, isholiday, traffic (based on the speed of the ride), etc.
    #Prepare data for sql processing
    raw_df.registerTempTable('trip_raw')
    sqlContext = SQLContext(spark.sparkContext)
    #Clean up: PULocationID is not null, passenger_count >0, trip_distance>0, total_amount>0
    df_trip_clean = sqlContext.sql('SELECT *, \
                             DATE(pickup_datetime) AS pickup_date, \
                             YEAR(pickup_datetime) AS pickup_year, \
                             MONTH(pickup_datetime) AS pickup_month, \
                             dayofmonth(pickup_datetime) AS pickup_day, \
                             CASE WHEN dayofweek(pickup_datetime) >5\
                             THEN 1\
                             ELSE 0\
                             END AS isweekend,\
                             HOUR(pickup_datetime) AS pickup_hour, \
                             total_amount-tip_amount AS fare_amount, \
                             ROUND((total_amount-tip_amount)/trip_distance,2) AS rate \
                             FROM trip_raw')
    #Get ride of rate higher than $50/mile, not reasonable.
    df_trip_clean = df_trip_clean.filter((df_trip_clean.rate <50))

    #calc speed and assign traffic condition, >25=1; 10-25=2; <10=3
    df_trip_clean = df_trip_clean.withColumn('duration',round((col('dropoff_datetime').cast(LongType()) - col('pickup_datetime').cast(LongType()))/3600,2))
    #Let duration larger than 0.1, which is 6min ride.
    df_trip_clean = df_trip_clean.filter((df_trip_clean.duration >0.1))
    df_trip_clean.registerTempTable('trip_clean')
    df_trip_traffic = sqlContext.sql('SELECT *, \
                             ROUND(trip_distance/duration,2) as speed,\
                             CASE WHEN trip_distance/duration >25\
                             THEN 1\
                             WHEN trip_distance/duration < 10\
                             THEN 3\
                             ELSE 2\
                             END AS traffic \
                             FROM trip_clean')
    #New york city has a speed limit as 25mph, but just in case
    df_trip_traffic = df_trip_traffic.filter((df_trip_traffic.speed <100) & (df_trip_traffic.speed >5))
    #Aggregate data by day, traffic
    df_trip_traffic.registerTempTable('trip_clean_traffic')#Get NY Holidays
    #Get year list from data
    df_years = df_trip_traffic.select('pickup_year').distinct().orderBy('pickup_year')
    df_combined = None
    for row in df_years.rdd.collect():
        holiday_NY = '","'.join([ date.strftime("%Y-%m-%d") for date, name in sorted(holidays.US(state='NY', years=row['pickup_year']).items())])
        #Non-holiday
        df_trip_nonholiday = sqlContext.sql('SELECT * FROM trip_clean_traffic WHERE pickup_year=' + str(row['pickup_year']) + ' AND \
                                                        pickup_date NOT IN ("' + holiday_NY + '")')
        df_trip_nonholiday = df_trip_nonholiday.withColumn('isholiday',lit(0))
        #Holiday
        df_trip_holiday = sqlContext.sql('SELECT * FROM trip_clean_traffic WHERE pickup_year=' + str(row['pickup_year']) + ' AND \
                                                        pickup_date IN ("' + holiday_NY + '")')
        df_trip_holiday = df_trip_holiday.withColumn('isholiday',lit(1)).withColumn('pickup_month',lit(0)).withColumn('isweekend',lit(-1))

        #concatenate current data with historical data in database.
        df_tmp = df_trip_holiday.union(df_trip_nonholiday)
        if df_combined is None:
            df_combined = df_tmp
        else:
            df_combined = df_combined.union(df_tmp)
    #Reorganize data
    cols = ['pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'fare_amount', \
            'pickup_date', 'pickup_year', 'pickup_month', 'pickup_day', 'pickup_hour', 'duration', 'speed', 'traffic', 'rate', 'isholiday','isweekend']
    return df_combined.select(cols)

def geo2taxizoneid(lat,lng):
    #Get taxi zone id based on given latitude and longitude.
    mysql = "SELECT tz.locationid FROM taxi_zones tz WHERE st_within(ST_SetSRID( ST_Point( "+str(lng)+","+str(lat)+"), 4326),tz.geom ) LIMIT 1"
    zoneid = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgis:5432/smartrider") \
        .option("query", mysql) \
        .option("user", "smartrider") \
        .option("password", "smartrider") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    if zoneid.count() !=1:
        #No taxizone was found
        return -1
    else:
        zoneid_list = zoneid.select('locationid').collect()
        return zoneid_list[0]['locationid']
def save2db(df,table):
    # write back to postgis
    df.write\
        .format("jdbc") \
        .mode("append") \
        .option("url", "jdbc:postgresql://postgis:5432/smartrider") \
        .option("dbtable", table) \
        .option("user", "smartrider") \
        .option("password", "smartrider") \
        .option("driver", "org.postgresql.Driver") \
        .save()

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: smartrider_traffic <file>", file=sys.stderr)
        sys.exit(-1)
    else:
        spark = SparkSession\
                .builder\
                .appName("smartrider")\
                .getOrCreate()
        spark.sparkContext.setLogLevel('ERROR')
        main(sys.argv[1])