DROP TABLE IF EXISTS RATES;
CREATE TABLE RATES(
        ZONEID SMALLINT NOT NULL DEFAULT 0,
        target_month SMALLINT NOT NULL DEFAULT 0,
        target_hour SMALLINT NOT NULL DEFAULT 0,
        traffic SMALLINT NOT NULL DEFAULT 0,
        rate REAL NOT NULL DEFAULT 0,
        isholiday SMALLINT NOT NULL DEFAULT 0,
        isweekend SMALLINT NOT NULL DEFAULT 0,
        iscovid SMALLINT NOT NULL DEFAULT 0
);
CREATE INDEX ON RATES (ZONEID, target_month, target_hour, traffic, isholiday, isweekend, iscovid);

/*Generate average rates for holidays, including all holidays before 2020, and before Mar. in 2020.*/
INSERT INTO RATES
SELECT PULocationID as ZONEID,
       0 as target_month,
       pickup_hour AS targt_hour,
       traffic,
       AVG(rate) as rate,
       1 as isholiday,
       -1 as isweekend,
       0 as iscovid
FROM TRIPDATA
WHERE isholiday = 1 AND (pickup_year <> 2020 OR (pickup_month<3 AND pickup_year = 2020))
GROUP BY PULocationID,pickup_hour,traffic
ORDER BY PULocationID,pickup_hour,traffic;

/*Generate average workday rates by month, hour, traffic, excluding all holidays, weekends, and after Mar. 2020.*/
INSERT INTO RATES
SELECT PULocationID as ZONEID,
       pickup_month AS target_month,
       pickup_hour AS targt_hour,
       traffic,
       AVG(rate) as rate,
       0 as isholiday,
       0 as isweekend,
       0 as iscovid
FROM TRIPDATA
WHERE isholiday < 1 AND isweekend < 1 AND (pickup_year <> 2020 OR (pickup_month<3 AND pickup_year = 2020))
GROUP BY PULocationID, pickup_month,pickup_hour,traffic
ORDER BY PULocationID, pickup_month,pickup_hour,traffic;

/*Generate average weekend rates by month, hour, traffic, excluding all holidays, weekends, and after Mar. 2020.*/
INSERT INTO RATES
SELECT PULocationID as ZONEID,
       pickup_month AS target_month,
       pickup_hour AS targt_hour,
       traffic,
       AVG(rate) as rate,
       0 as isholiday,
       1 as isweekend,
       0 as iscovid
FROM TRIPDATA
WHERE isweekend = 1 AND isholiday < 1 AND (pickup_year <> 2020 OR (pickup_month<3 AND pickup_year = 2020))
GROUP BY PULocationID, pickup_month,pickup_hour,traffic
ORDER BY PULocationID, pickup_month,pickup_hour,traffic;

/*******************************************************
COVID related processing
********************************************************/

/*Generate average rates for holidays, after Mar 2020.*/
INSERT INTO RATES
SELECT PULocationID as ZONEID,
       -1 as target_month,
       pickup_hour AS targt_hour,
       traffic,
       AVG(rate) as rate,
       1 as isholiday,
       -1 as isweekend,
       1 as iscovid
FROM TRIPDATA
WHERE isholiday = 1 AND (pickup_year > 2020 OR (pickup_month > 2 AND pickup_year = 2020))
GROUP BY PULocationID,pickup_hour,traffic
ORDER BY PULocationID,pickup_hour,traffic;

/*Generate average workday rates by month, hour, traffic, excluding all holidays, weekends, after Mar. 2020.*/
INSERT INTO RATES
SELECT PULocationID as ZONEID,
       pickup_month AS target_month,
       pickup_hour AS targt_hour,
       traffic,
       AVG(rate) as rate,
       0 as isholiday,
       0 as isweekend,
       1 as iscovid
FROM TRIPDATA
WHERE isholiday < 1 AND isweekend < 1 AND (pickup_year > 2020 OR (pickup_month > 2 AND pickup_year = 2020))
GROUP BY PULocationID, pickup_month,pickup_hour,traffic
ORDER BY PULocationID, pickup_month,pickup_hour,traffic;

/*Generate average weekend rates by month, hour, traffic, excluding all holidays, weekends, after Mar. 2020.*/
INSERT INTO RATES
SELECT PULocationID as ZONEID,
       pickup_month AS target_month,
       pickup_hour AS targt_hour,
       traffic,
       AVG(rate) as rate,
       0 as isholiday,
       1 as isweekend,
       1 as iscovid
FROM TRIPDATA
WHERE isweekend = 1 AND isholiday < 1 AND (pickup_year > 2020 OR (pickup_month > 2 AND pickup_year = 2020))
GROUP BY PULocationID, pickup_month,pickup_hour,traffic
ORDER BY PULocationID, pickup_month,pickup_hour,traffic;
