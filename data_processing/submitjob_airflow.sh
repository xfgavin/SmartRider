#!/usr/bin/env bash
##############################################
#spark job submitor for airflow for the Smart Rider project
# By Feng Xue @RB Insight DE 20C
##############################################
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON=/usr/bin/python3
Usage(){
[ ${#Error} -gt 0 ] && echo -e "\nError: $Error\n"
cat <<EOF
++++++++++++++++++++++++++++++++++++++++++++++++++++
Spark job submitter for airflow for the smartrider project
by xfgavin@gmail.com 09/30/2020 @RB
+++++++++++++++++++++++++++++++++++++++++++++++++++++
usage: `basename $0` options
OPTIONS:
   -i      input file name
Example:
  `basename $0` -i yellow_tripdata_2020-05.csv
EOF
[ ${#Error} -gt 0 ] && exit -1
exit 0
}
while getopts "i:" OPTION
do
  case $OPTION in
    i)
	inputfile="$OPTARG"
      ;;
    *)
      Usage
      exit
      ;;
  esac
done

[ ${#inputfile} -eq 0 ] && Usage
echo "Importing $inputfile"
spark-submit --driver-class-path /opt/spark/jars/postgresql-42.2.16.jar \
             --jars /opt/spark/jars/postgresql-42.2.16.jar \
             --packages com.amazonaws:aws-java-sdk-bundle:1.11.876,org.apache.hadoop:hadoop-aws:3.2.0 \
             --master spark://spark-master:7077 \
             smartrider_process_data.py "s3a://nyc-tlc/trip data/$inputfile"
