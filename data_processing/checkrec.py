#!/usr/bin/env python3
import psycopg2
import sys
import os
from datetime import datetime as dt
import boto3
from botocore import UNSIGNED
from botocore.config import Config

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
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    
    #Validate existence on S3
    tasklist = filtertasklist(tasklist,s3)
    
    #Execute shell script to submit spark jobs
    for task in tasklist:
        print('Importing: ' + task)
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
        
    #islocked:
    #0: no data imported
    #1: completeness not checked
    #2: good
    mysql = "select filename from tripdata_filename where filename like '" + keyword + "%' and islocked=2 order by filename"
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
    
    
def filtertasklist(tasklist,s3obj):
    newtasklist = tasklist.copy()
    for key in tasklist:
        keyexist = 0
        prefix = 'trip data/' +key
        response = s3obj.list_objects_v2(
                Bucket='nyc-tlc',
                Prefix=prefix,
        )
        for obj in response.get('Contents', []):
            if obj['Key'] == prefix:
                keyexist = 1
        if keyexist == 0:
            newtasklist.remove(key)
    return newtasklist
    
if __name__ == '__main__':
    main()
