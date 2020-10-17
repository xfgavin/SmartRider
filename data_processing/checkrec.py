#!/usr/bin/env python3
import psycopg2
import sys
import os
from datetime import datetime as dt
import boto3
from botocore.handlers import disable_signing

def main():
    #This function will:
    # 1. generate filelist and compare with files in tripdata_filename table.
    # 2. based on filelist, call submitjob_airflow.sh to submit jobs to Spark
    
    #Create dabase connection
    conn = connectDB()
    
    #get list of files haven't been processed yet
    tasklist = gen_tasklist(1,conn) + gen_tasklist(0,conn)
    
    #close database connection
    closeDB(conn)
    
    #Create S3 client with no signing
    s3 = boto3.resource('s3')
    s3.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)
    bucket = s3.Bucket('nyc-tlc')
    
    #Validate existence on S3
    tasklist = filtertasklist(tasklist,bucket)
    
    #Execute shell script to submit spark jobs
    for task in tasklist:
        os.system(os.getcwd()+'/submitjob_airflow.sh -i ' + task)
        
def getdata(sql,conn):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        result = list(sum(cur.fetchall(), ()))
        cur.close()
        return result
    except:
        conn.rollback()
        cur.close()
        return []

def gen_tasklist(isYellow,conn):
    if isYellow:
        keyword = 'yellow'
        year_start=2009
    else:
        keyword = 'green'
        year_start=2013
        
    mysql = "select filename from tripdata_filename where filename like '" + keyword + "%' order by filename"
    rec = getdata(mysql,conn)
    return [keyword + '_tripdata_' + str(year) + '-' + f'{month:02}' + '.csv' for year in range(year_start,dt.now().year+1) for month in range(1,13) if year != dt.now().year or month<=dt.now().month if keyword + '_tripdata_' + str(year) + '-' + f'{month:02}' + '.csv' not in rec]

def connectDB():
    try:
        conn = psycopg2.connect(
               host=os.environ['PGHOST'],
               database=os.environ['PGDB'],
               user=os.environ['PGUSER'],
               password=os.environ['PGPWD'])
        return conn
    except:
        print('Can not connect to the database!')
        sys.exit(-1)

def closeDB(conn):
    conn.close()
    
    
def filtertasklist(tasklist,bucket):
    for key in tasklist:
        prefix = 'trip data/' + key
        objs = list(bucket.objects.filter(Prefix=prefix))
        if not any([w.key == key for w in objs]):
            tasklist.remove(key)
    return tasklist
    
if __name__ == '__main__':
    main()
