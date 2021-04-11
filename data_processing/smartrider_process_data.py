#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import *
import sys
import os
import holidays
import re
from datetime import date

def main(s3file, sourceid):
    '''
    This function will:
     1. Load data from s3
     2. Give raw data a good masssage
     3. save to database
    '''
    #s3file='yellow_tripdata_2020-04.csv'
    raw_df = read_clean_csv(s3file)
    tripdata_df = process_tripdata(raw_df,sourceid)
    
    if "pickup_longitude" in tripdata_df.schema.names:
        save2db(tripdata_df,'tripdata_geo')
    else:
        save2db(tripdata_df,'tripdata')
    
def create_holiday_df():
    '''
    Create a holiday dataframe containing holidays ranging from 2008 to current year +1
    Since this dataframe is small, broadcast join will be used to join with raw_df
    '''
    data = []
    schema = ['holiday_year','holiday_month','holiday_day','isholiday']
    for thisyear in range(2008,date.today().year+2):
        for holiday_date, holiday_name in sorted(holidays.US(state='NY', years=thisyear).items()):
            thisholiday = list(map(int, str(holiday_date).split('-')))
            data.append((thisholiday[0],thisholiday[1],thisholiday[2],1))
    holiday_df = spark.createDataFrame(data).toDF(*schema)
    return holiday_df
def removeLastColumnSpecialChar(df):
    '''
    To remove special characters from last column.
    Usually, only last column has special characters, might be due to editing under windows.
    It's usually again last character.
    '''
    lastcol=df.schema.names[-1]
    lastcol_list=re.split(r'(\W+)',lastcol)
    if len(lastcol_list)>1:
        df = df.withColumnRenamed(lastcol, lastcol_list[0])
    return df
def removeColumnSpecialChar(df):
    '''
    To remove special characters from last column.
    Usually, only last column has special characters, might be due to editing under windows.
    It's usually again last character.
    '''
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

def read_clean_csv(s3file):
    '''
    This function will:
      1. load raw NYC taxi trip data from S3
      2. unify column names
      3. clean up
    '''
    ##############
    #Removed udf and use sql instead
    ##############
    #setisweekend_udf = udf(setisweekend)
    #settraffic_udf = udf(settraffic)
    raw_df = spark.read.csv(s3file, header=True, inferSchema=True, multiLine=True, escape='"')        
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
    #Clean up: PULocationID is not null, passenger_count >0, trip_distance>0, total_amount>0
    raw_df = raw_df.filter("pickup_datetime != ''")
    raw_df = raw_df.filter("dropoff_datetime != ''")
    raw_df = raw_df.filter("passenger_count >0")
    raw_df = raw_df.filter("trip_distance >0.5")
    raw_df = raw_df.filter("tip_amount >=0")
    raw_df = raw_df.filter("total_amount >=2.5") #NYC taxi minimal charge
    #minimal fare is based on https://www1.nyc.gov/site/tlc/passengers/taxi-fare.page
    
    raw_df = raw_df.select(
        '*',
        (col('total_amount')-col('tip_amount')).alias('fare_amount'),
        to_date(col('pickup_datetime')).alias('pickup_date'),
        to_timestamp(col('pickup_datetime')).alias('pickup_datetime'),
        to_timestamp(col('dropoff_datetime')).alias('dropoff_datetime')
    )
    raw_df = raw_df.select(
        '*',
        round(col('fare_amount')/col('trip_distance'),2).alias('rate'),
        round((col('dropoff_datetime').cast(LongType()) - col('pickup_datetime').cast(LongType()))/3600,2).alias('duration'),
        year(col('pickup_datetime')).alias('pickup_year'),
        month(col('pickup_datetime')).alias('pickup_month'),
        dayofmonth(col('pickup_datetime')).alias('pickup_day'),
        dayofweek(col('pickup_datetime')).alias('pickup_weekday'),
        hour(col('pickup_datetime')).alias('pickup_hour')
    )
    
    #Get ride of rate higher than $50/mile, not reasonable.
    #Let duration larger than 0.1, which is 6min ride.
    raw_df = raw_df.filter("rate <50")
    raw_df = raw_df.filter("duration >0.1")
    #Data should be between 2008 to now+1year
    raw_df = raw_df.filter("pickup_year >2007 and pickup_year <year(current_date())+1")
    
    raw_df = raw_df.select(
        '*',
        round(col('trip_distance')/col('duration'),2).alias('speed'),
        when((col('pickup_weekday')==1) | (col('pickup_weekday')==7), 1).otherwise(0).alias('isweekend')
    )
        
    #New york city has a speed limit as 25mph, but just in case
    raw_df = raw_df.filter("speed >5 and speed < 100")
    raw_df = raw_df.select(
        '*',
        when(col('speed')>25, 1)
        .when(col('speed')<10, 3).otherwise(2).alias('traffic')
    )

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
    '''
    This function uses roadcast join to assign the isholiday column to the dataframe.
    '''
    holiday_df = create_holiday_df()
    tripdata_df = raw_df.join(broadcast(holiday_df),(holiday_df.holiday_year==raw_df.pickup_year) & (holiday_df.holiday_month==raw_df.pickup_month) & (holiday_df.holiday_day==raw_df.pickup_day),how="left")\
                        .drop('holiday_year','holiday_month','holiday_day')
    tripdata_df = tripdata_df.withColumn('pickup_month', when(tripdata_df['isholiday']==1,0).otherwise(tripdata_df['pickup_month'])) \
                             .withColumn('isweekend', when(tripdata_df['isholiday']==1,-1).otherwise(tripdata_df['isweekend'])) \
                             .na.fill(0,'isholiday') \ 
                             .withColumn('sourceid',lit(sourceid))
    
    if "pickup_longitude" in tripdata_df.schema.names:
        cols = ['pickup_datetime', 'dropoff_datetime', 'pickup_latitude', 'pickup_longitude', 'dropoff_latitude','dropoff_longitude', 'passenger_count', 'trip_distance', 'fare_amount', \
                'pickup_date', 'pickup_year', 'pickup_month', 'pickup_day', 'pickup_hour', 'duration', 'speed', 'traffic', 'rate', 'isholiday','isweekend', 'sourceid']
    else:
        cols = ['pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'fare_amount', \
                'pickup_date', 'pickup_year', 'pickup_month', 'pickup_day', 'pickup_hour', 'duration', 'speed', 'traffic', 'rate', 'isholiday','isweekend', 'sourceid']
    
    return tripdata_df.select(cols)

def geo2taxizoneid(lat,lng):
    '''
    Get taxi zone id based on given latitude and longitude.
    Outdated, used stored procedure in postgis instead.
    '''
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
    '''
    write back to postgis
    '''
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
    '''
    #############
    Register input filename for reference in trip_data table
    #############
    '''
    input_filename = os.path.basename(s3file).lower()
    data = [tuple([input_filename])]
    schema = ['filename']
    df = spark.createDataFrame(data).toDF(*schema)
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