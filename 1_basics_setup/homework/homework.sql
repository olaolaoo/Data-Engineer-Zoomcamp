SELECT *
FROM green
LIMIT 10

#question3
SELECT count(1)
FROM green
WHERE DATE(lpep_pickup_datetime) = '2019-09-18'
    AND DATE(lpep_dropoff_datetime) = '2019-09-18'

#question4
SELECT DATE(lpep_pickup_datetime),sum(trip_distance) AS total
FROM green
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY total DESC
LIMIT 1

DROP TABLE green

SELECT column_name
FROM information_schema.columns
WHERE table_name = 'your_table_name';

SELECT zone."Borough", SUM(green."total_amount") AS amount
FROM green
LEFT JOIN zone ON green."index" = zone."index" 
WHERE zone."Borough" <> 'Unknown' AND DATE(green."lpep_pickup_datetime")='2019-09-18'
GROUP BY zone."Borough"
HAVING SUM(green."total_amount") > 50000;

SELECT t2."Zone", 
        sum(t1."tip_amount") AS amount
FROM
(
    SELECT green."tip_amount" AS "tip_amount",green."DOLocationID" AS "DOLocationID"
    FROM green
    LEFT JOIN zone ON green."PULocationID" = zone."LocationID" 
    WHERE DATE(green."lpep_pickup_datetime") BETWEEN '2019-09-01' AND '2019-09-30'
        AND zone."Zone" ='Astoria'
)t1
LEFT JOIN
(
    SELECT zone."Zone" AS "Zone",zone."LocationID" AS "LocationID" 
    FROM zone
)t2
ON t1."DOLocationID"=t2."LocationID"
GROUP BY 1
ORDER BY amount DESC
LIMIT 1;
