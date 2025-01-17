<img src="https://github.com/xfgavin/SmartRider/blob/master/images/rainbowbar_title.png?raw=true" width="100%">
Ride smarter, for less!
<img src="https://github.com/xfgavin/SmartRider/blob/master/images/icons.png?raw=true">

Ride share market has grown exponentially during past a few years, so does the need for a ride share planning platform. When you plan a trip, you can conveniently refer to Google Flight search, and get better idea of when and where to start the trip. Unfortunately, there is no such a system for rideshare planning yet.
## Table of Contents
* [Solution](#Solution)
* [The Dataset](#Dataset)
* [Tech Stack](#Techstack)
* [Data Structure](#Datastructure)
* [Highlights](#Highlights)
   * [Data Conversion](#Dataconversion)
   * [Data Completeness Check](#Datacheck)
* [Cluster Configuration](#Setup)
* [Future Directions](#Futuredirections)
* [Slides](#Slides)

### <a name="Solution">Solution</a>
Here I would like to propose a data driven solution: [SmartRider](https://smartrider.dtrace.net). 
<img src="https://github.com/xfgavin/SmartRider/blob/master/images/snapshot.png?raw=true">

It is based on historical taxi trip data. Users can pick a location on the map, then adjust parameters like trip month, traffic situation, and social restrictions such as COVID.

### <a name="Dataset">The dataset</a>
[New York city taxi trip data](https://registry.opendata.aws/nyc-tlc-trip-records-pds/).
This dataset (yellow & green taxis only) has:

* taxi trip from 2009 to present,
* includes either pickup/dropoff geo location or [taxi zone id](https://s3.amazonaws.com/nyc-tlc/misc/taxi_zones.zip).
* file size: ~240GB
* updates monthly

### <a name="Techstack">Tech stack</a>
<img src="https://github.com/xfgavin/SmartRider/blob/master/images/techstack.png?raw=true">
This pipeline gets data from S3, ETLs data using a Spark cluster and saves data to PostgreSQL database with PostGIS extension. Finally, Dash from plotly is used to provide web service. Airflow is used to schedule data processing jobs when new data file exists.
Software packages/Tools used in this project

* Apache Spark, ver: 3.0.1<img src="https://spark.apache.org/images/spark-logo-trademark.png" height="40px">
* Apache Airflow, ver: 1.10.12<img src="https://airflow.apache.org/images/feature-image.png" height="40px">
* PostGreSQL with PostGIS extension, ver: 12.4<img src="https://postgis.net/images/postgis-logo.png" height="40px">
* Plotly Dash, ver: 1.16.2<img src="https://www.educative.io/api/edpresso/shot/6166549980250112/image/5979145793175552" height="40px">
* Dash leaflet, ver: 0.1.4<img src="http://dash-leaflet.herokuapp.com/assets/leaflet.png" height="40px">

### <a name="Datastructure">Data structure</a>
Farely simple:

<img src="https://github.com/xfgavin/SmartRider/blob/master/images/db_diag.png?raw=true">

### <a name="Highlights">Highlights</a>
* <a name="Dataconversion">Efficient way in geo location conversion.</a> Data before 2017 has pickup geo locations (point) with longitude and latitude, but data since 2017 only has pickup taxi zone id (area). To do the conversion, PostGIS is used because it has lots of geo related functions. Here are two options to do the conversion:
   * #1: convert during Spark ETL, query DB for each geo pair.
   * #2: create a stored procedure in Postgres and convert inside database after Spark ETL
   For a 15M rows of csv:
   
   Option #1 took >2days (ETL job was killed after two days of running).
   
   Option #2 18min (ETL) + 1.1ms per record * 15M ~=5hr.
   So Option #2 is more than 100 times faster. This is because Option #1 has to deal with Spark JDBC, TCP connection, network transportation, and database query for each pair of geo data while Option #2 only need to deal with database query.

* <a name="Datacheck">Data completeness check.</a> During Spark ETL, some job failed because of various reasons (bad format, job stuck in queue too long, etc.). So data completeness check is necessary to make sure all data is imported completely. The criteria used to consider a csv was imported completely is the tripdata has >1000 records for the csv. Here are two approaches:
   * #1: Loop all records in csv filename table and count(id) in tripdata.
   * #2: For data in tripdata table, count 10K rows by 10K rows, and label a csv was completely imported if it has at least 1000 records in a given 10K row. Afterwards, use approach #1 to check unlabeled csv files.
   
   Approach #1 took 47hrs to finish (1.4B rows in tripdata)
   
   Approach #2 took 2hrs to check all the 1.4B rows in tripdata and labeled 200 good csvs, then Approach #1 was used to check unlabeled csv (35 total), which took another 7hrs. In total, approach #2 used 9 hrs.
   
   So Approach #2 is about 5 times faster than approach #1.

### <a name="Setup">Set up</a>
Ansible and docker are used to setup spark cluster in Amazon AWS for this project. Please check [/setup/playbook](/setup/playbook)

Please also find scripts for database setup in [/db](/db)


### <a name="Futuredirections">Future Directions</a>
* Data enrichment
  * add data from other providers, such as uber, lyft etc.
  * add data from other cities
  * increase granularities when setting search parameters
* Database Optimization: because tripdata is computational intensive(geo conversion, rates calculation, etc.), it could negatively affect user experience.
  * Dedicated tripdata database server
  * Dedicated rates database server
* Database server file system optimization: Since Postgres saves smaller files on disk(~1G), file system such as xfs might be more efficient, ext4 is used in this project though.
  * ext4 vs xfs
  
### <a name="Slides">Slides</a>
[Google Slides](https://docs.google.com/presentation/d/1TcylCo1eHtIEO8UI1zvEn9nlOYM_JZMWL2prc-WusVQ/edit?usp=sharing)
