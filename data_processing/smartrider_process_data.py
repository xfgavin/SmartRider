#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SQLContext, Row
import sys
import os
import holidays
import re

def main(s3file, sourceid):
    #This function will:
    # 1. Load data from s3
    # 2. Give raw data a good masssage
    # 3. save to database
    #s3file='yellow_tripdata_2020-04.csv'
    raw_df = read_clean_csv(s3file)
    tripdata_df = process_tripdata(raw_df,sourceid)
    
    if "pickup_longitude" in tripdata_df.schema.names:
        save2db(tripdata_df,'tripdata_geo')
    else:
        save2db(tripdata_df,'tripdata')
    
def removeLastColumnSpecialChar(df):
    #########################
    #To remove special characters from last column.
    #Usually, only last column has special characters, might be due to editing under windows.
    #It's usually again last character.
    lastcol=df.schema.names[-1]
    lastcol_list=re.split(r'(\W+)',lastcol)
    if len(lastcol_list)>1:
        df = df.withColumnRenamed(lastcol, lastcol_list[0])
    return df
def removeColumnSpecialChar(df):
    #########################
    #To remove special characters from last column.
    #Usually, only last column has special characters, might be due to editing under windows.
    #It's usually again last character.
    for col in df.schema.names:
        col_list=re.split(r'(\W+)',col)
        if len(col_list)>1:
            maxlen = 0
            realcol = []
            for item in col_list:
                if len(item)>maxlen:
                    maxlen = len(item)
                    realcol = item
            df = df.withColumnRenamed(col, realcol)
    return df

def setisweekend(myday):
    if myday>1 and myday<7:
        return 0
    else:
        return 1

def settraffic(speed):
    #calc speed and assign traffic condition, >25=1; 10-25=2; <10=3
    if speed>25:
        return 1
    elif speed <10:
        return 3
    else:
        return 2

def read_clean_csv(s3file):
    #This function will:
    #  1. load raw NYC taxi trip data from S3
    #  2. unify column names
    #  3. clean up
    setisweekend_udf = udf(setisweekend)
    settraffic_udf = udf(settraffic)
    raw_df = spark.read.csv(s3file, header = True, inferSchema=True, multiLine=True, escape='"')        
    raw_df = removeColumnSpecialChar(raw_df)
    raw_df = raw_df.withColumnRenamed("Trip_Pickup_DateTime", "pickup_datetime") \
                   .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
                   .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
                   .withColumnRenamed("Trip_Dropoff_DateTime", "dropoff_datetime") \
                   .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
                   .withColumnRenamed("Lpep_dropoff_datetime", "dropoff_datetime") \
                   .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime") \
                   .withColumnRenamed("Passenger_Count", "passenger_count") \
                   .withColumnRenamed("Passenger_count", "passenger_count") \
                   .withColumnRenamed("Trip_Distance", "trip_distance") \
                   .withColumnRenamed("Trip_distance", "trip_distance") \
                   .withColumnRenamed("Tip_Amt", "tip_amount") \
                   .withColumnRenamed("Tip_amount", "tip_amount") \
                   .withColumnRenamed("Total_Amt", "total_amount") \
                   .withColumnRenamed("Total_amount", "total_amount") \
                   .withColumnRenamed("Start_Lat", "pickup_latitude") \
                   .withColumnRenamed("Start_Lon", "pickup_longitude") \
                   .withColumnRenamed("End_Lat", "dropoff_latitude") \
                   .withColumnRenamed("End_Lon", "dropoff_longitude") \
                   .withColumnRenamed("Pickup_latitude", "pickup_latitude") \
                   .withColumnRenamed("Pickup_longitude", "pickup_longitude") \
                   .withColumnRenamed("Dropoff_latitude", "dropoff_latitude") \
                   .withColumnRenamed("Dropoff_longitude", "dropoff_longitude")
    raw_df = raw_df.filter("pickup_datetime != ''")
    raw_df = raw_df.filter("dropoff_datetime != ''")
    raw_df = raw_df.filter("passenger_count >0")
    raw_df = raw_df.filter("trip_distance >0.5")
    raw_df = raw_df.filter("tip_amount >=0")
    raw_df = raw_df.filter("total_amount >=2.5") #NYC taxi minimal charge
    
    raw_df = raw_df.withColumn('fare_amount', col('total_amount')-col('tip_amount')) \
                   .withColumn('pickup_date',to_date(col('pickup_datetime'))) \
                   .withColumn('pickup_datetime',to_timestamp(col('pickup_datetime'))) \
                   .withColumn('dropoff_datetime', to_timestamp(col('dropoff_datetime')))
    
    raw_df = raw_df.withColumn('rate', round(col('fare_amount')/col('trip_distance'),2)) \
                   .withColumn('duration',round((col('dropoff_datetime').cast(LongType()) - col('pickup_datetime').cast(LongType()))/3600,2)) \
                   .withColumn('pickup_year', year(col('pickup_datetime'))) \
                   .withColumn('pickup_month', month(col('pickup_datetime'))) \
                   .withColumn('pickup_day', dayofmonth(col('pickup_datetime'))) \
                   .withColumn('pickup_hour', hour(col('pickup_datetime'))) \
                   .withColumn('isweekend', setisweekend_udf(dayofweek(col('pickup_datetime'))).cast(IntegerType()))
    raw_df = raw_df.withColumn('speed', round(col('trip_distance')/col('duration'),2)) \
                   .withColumn('traffic', settraffic_udf(col('speed')).cast(IntegerType()))
    
    raw_df = raw_df.filter("rate <50")
    raw_df = raw_df.filter("duration >0.1")
    raw_df = raw_df.filter("speed >5")
    raw_df = raw_df.filter("speed <100")
    
    
    #New york city has a speed limit as 25mph, but just in case
    #Cleaning up data with invalid values for a given column.
    #minimal fare is based on https://www1.nyc.gov/site/tlc/passengers/taxi-fare.page
    #Clean up: PULocationID is not null, passenger_count >0, trip_distance>0, total_amount>0
    #Get ride of rate higher than $50/mile, not reasonable.
    #Let duration larger than 0.1, which is 6min ride.


    if "pickup_longitude" in raw_df.schema.names:
        raw_df = raw_df.filter((raw_df.pickup_latitude != 0) & (raw_df.pickup_longitude != 0))
        ##########################
        #Huge performance cost, use stored procedure instead.
        #raw_df['PULocationID'] = [geo2taxizoneid(pickup_latitude,pickup_longitude) for pickup_latitude, pickup_longitude in zip(raw_df['pickup_latitude'], raw_df['pickup_longitude'])]
        #raw_df['DOLocationID'] = [geo2taxizoneid(dropoff_latitude,dropoff_longitude) for dropoff_latitude, dropoff_longitude in zip(raw_df['dropoff_latitude'], raw_df['dropoff_longitude'])]
        #Reorganize data
        cols = ['pickup_datetime', 'dropoff_datetime', 'pickup_latitude', 'pickup_longitude', 'dropoff_latitude','dropoff_longitude', 'passenger_count', 'trip_distance', 'fare_amount', \
                'pickup_date', 'pickup_year', 'pickup_month', 'pickup_day', 'pickup_hour', 'duration', 'speed', 'traffic', 'rate','isweekend']
    else:
        #Only include valid PUlocationID.
        raw_df = raw_df.filter((raw_df.PULocationID >0))
        #Reorganize data
        cols = ['pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'fare_amount', \
                'pickup_date', 'pickup_year', 'pickup_month', 'pickup_day', 'pickup_hour', 'duration', 'speed', 'traffic', 'rate','isweekend']
    
    return raw_df.select(cols)

def process_tripdata(raw_df, sourceid):
    #This function uses spark.sql to compute some fields, including pickup_date, pickup_year, pickup_month, pickup_day, pickup_hour, duration, rate, isweekend, isholiday, traffic (based on the speed of the ride), etc.
    #Prepare data for sql processing
    sqlContext = SQLContext(spark.sparkContext)
    #Aggregate data by day, traffic
    raw_df.registerTempTable('trip_clean_traffic')#Get NY Holidays
    #Get year list from data
    df_years = raw_df.select('pickup_year').distinct().orderBy('pickup_year')
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
    df_combined = df_combined.withColumn('sourceid',lit(sourceid))
    
    if "pickup_longitude" in df_combined.schema.names:
        cols = ['pickup_datetime', 'dropoff_datetime', 'pickup_latitude', 'pickup_longitude', 'dropoff_latitude','dropoff_longitude', 'passenger_count', 'trip_distance', 'fare_amount', \
                'pickup_date', 'pickup_year', 'pickup_month', 'pickup_day', 'pickup_hour', 'duration', 'speed', 'traffic', 'rate', 'isholiday','isweekend', 'sourceid']
    else:
        cols = ['pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'fare_amount', \
                'pickup_date', 'pickup_year', 'pickup_month', 'pickup_day', 'pickup_hour', 'duration', 'speed', 'traffic', 'rate', 'isholiday','isweekend', 'sourceid']
    
    return df_combined.select(cols)

def geo2taxizoneid(lat,lng):
    #Get taxi zone id based on given latitude and longitude.
    ######
    #outdated, used stored procedure in postgis instead.
    mysql = "SELECT tz.locationid FROM taxi_zones tz WHERE st_within(ST_SetSRID( ST_Point( "+str(lng)+","+str(lat)+"), 4326),tz.geom ) LIMIT 1"
    zoneid = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgis:5432/" + os.environ['PGDB']) \
        .option("query", mysql) \
        .option("user", os.environ['PGUSER']) \
        .option("password", os.environ['PGPWD']) \
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
        .option("url", "jdbc:postgresql://postgis:5432/" + os.environ['PGDB']) \
        .option("dbtable", table) \
        .option("user", os.environ['PGUSER']) \
        .option("password", os.environ['PGPWD']) \
        .option("driver", "org.postgresql.Driver") \
        .save()
    
def getSourceID(s3file,table):
    #############
    #Register input filename for reference in trip_data table
    #############
    input_filename = os.path.basename(s3file).lower()
    rec = Row("filename")
    rec = rec(input_filename)
    df = spark.createDataFrame(Row(rec))
    try:
        df.write\
                .format("jdbc") \
                .mode("append") \
                .option("url", "jdbc:postgresql://postgis:5432/" + os.environ['PGDB']) \
                .option("dbtable", table) \
                .option("user", os.environ['PGUSER']) \
                .option("password", os.environ['PGPWD']) \
                .option("driver", "org.postgresql.Driver") \
                .save()
        mysql = "SELECT ID FROM " + table + " WHERE FILENAME='" + input_filename + "'"
        sourceid = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgis:5432/" + os.environ['PGDB']) \
            .option("query", mysql) \
            .option("user", os.environ['PGUSER']) \
            .option("password", os.environ['PGPWD']) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        sourceid_list = sourceid.select('id').collect()
        return sourceid_list[0]['id']
    except:
        return -1

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: smartrider_traffic <file>", file=sys.stderr)
        sys.exit(-1)
    else:
        s3file = sys.argv[1]
    #s3file='/opt/tmp2.csv'
    #s3file='s3a://nyc-tlc/trip data/green_tripdata_2020-04.csv'
    
    conf = SparkConf() \
        .set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
        .set('spark.executor.memory', '3g') \
        .set('spark.executor.cores', 2) \
        .set('spark.sql.files.maxPartitionBytes', 128*1024*1024) \
        .set('spark.sql.shuffle.partitions', 64)
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    spark = SparkSession\
            .builder\
            .appName("smartrider")\
            .getOrCreate()
    ##############
    #Register filename into database to make sure same file won't be processed multiple times.
    ##############
    sourceid = getSourceID(s3file,'tripdata_filename')
    if sourceid==-1:
        print('Error when retrieving sourceid')
        sys.exit(-1)
    main(s3file,sourceid)