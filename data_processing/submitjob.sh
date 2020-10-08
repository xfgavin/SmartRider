#!/usr/bin/env bash
##############################################
#spark job submitor for Smart Rider project
# By Feng Xue @RB Insight DE 20C
##############################################
#Loop over year and month to process data
##############################################

#for year in `seq 2009 2019`
for year in 2019
do
  for month in `seq 12`
  #for month in `seq 6`
  do
    [ ${#month} -eq 1 ] && monthstring="0$month"
    [ ${#month} -eq 2 ] && monthstring="$month"
    spark-submit --driver-class-path /opt/spark/jars/postgresql-42.2.16.jar \
  	       --jars /opt/spark/jars/postgresql-42.2.16.jar \
  	       --packages com.amazonaws:aws-java-sdk-bundle:1.11.876,org.apache.hadoop:hadoop-aws:3.2.0 \
  	       --master spark://spark-master:7077 \
  	       smartrider_process_data.py "s3a://nyc-tlc/trip data/yellow_tripdata_${year}-$monthstring.csv" &
  done
done

#V4 authentication
#	     --conf spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true \
#	     --conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true
