# 
# SmartRider
<img src="https://github.com/xfgavin/SmartRider/blob/master/images/rainbowbar.png?raw=true" width="100%">
Ride smarter, for less!
<img src="https://github.com/xfgavin/SmartRider/blob/master/images/icons.png?raw=true">

Ride share market has grown expontionally during past a few years, so does the need for a ride share planning platform. When you plan a trip, you can conveniently refer to Google Flight search, and get better idea of when and where to start the trip. Unfortunately, there is no such a system for rideshare planning yet.

## The solution
Here I would like to propose a data driven solution: [SmartRider](https://smartrider.dtrace.net). 
<img src="https://github.com/xfgavin/SmartRider/blob/master/images/snapshot.png?raw=true">

It is based on historical taxi trip data. Users can pick a location on the map, then adjust parameters like trip month, traffic situation, and social restrictions such as COVID.

Under the hood, I used New York city taxi trip data (yellow & green taxis only) and a Spark centered pipeline.

## The data
<img src="https://www1.nyc.gov/assets/tlc/images/content/pages/home/nyc-tlc-logo.png">
[New York city taxi trip data](https://registry.opendata.aws/nyc-tlc-trip-records-pds/).
This dataset has:
1. taxi trip from 2009 to present,
2. includes either pickup/dropoff geo location or [taxi zone id]https://s3.amazonaws.com/nyc-tlc/misc/taxi_zones.zip.
3. file size: ~240GB

## The tech stack
<img src="https://github.com/xfgavin/SmartRider/blob/master/images/techstack.png?raw=true">
