This folder contains scripts for data processing using spark:
1. smartrider_process_data.py: Given a new york taxi trip data csv file, this script will extract necessary data from raw csv files.
2. submitjob.sh: This script submits jobs to spark cluster.
3. checkrec.sh, checkrec.py, and submitjob_airflow.sh are for airflow tasks.checkrec.sh is mainly used to set up shell environment, checkrec.py is used to check which csv should be processed by comparing records in tripdata_filename and S3. submitjob_airflow.sh is basically a spark job submittor for a single csv file.
