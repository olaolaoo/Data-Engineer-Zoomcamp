## Week 1 Homework

# Docker & SQL

## Question 1. Knowing docker tags

> **Answer:--rm**

Run the command to get information on Docker

Which tag has the following text? - *Automatically remove the container when it exits*

- `--delete`
- `--rc`
- `--rmc`
- `--rm`

run `docker --help`


## Question 2. Understanding docker first run

> **Answer: wheel      0.42.0**

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash. Now check the python modules that are installed ( use `pip list`)

to answer this quetion, Run the command `docker run -it --entrypoint=bash python:3.9`, and type `pip list` into the terminal

```
(base) ola@oladeMacBook-Pro ~ % docker run -it --entrypoint=bash python:3.9
root@3a1462f4b474:/# pip list
Package    Version
---------- -------
pip        23.0.1
setuptools 58.1.0
wheel      0.42.0
```

# Prepare Postgres 

We'll use the green taxi trips from September 2019:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz
```

You will also need the dataset with zones:

```bash 
wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
```

>Command:
```bash
# Create two new ingest scripts that ingest both files called ingest_data_green.py and ingest_data_zone.py

# Run Postgres and Pgadmin container,from the working directory where docker-compose.yaml is
docker-compose up

# Finally, run the script seperately 
python ingest_data_green.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green \
    --url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"

python ingest_data_zone.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=zone \
    --url="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
```

## Question 3. Count records 

How many taxi trips were totally made on September 18th 2019?

User pgadmin

>Command:
```sql
SELECT count(1)
FROM green
WHERE DATE(lpep_pickup_datetime) = '2019-09-18'
    AND DATE(lpep_dropoff_datetime) = '2019-09-18'
```
>Anwer:
```
15612
```
## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance? Use the pick up time for your calculations.

Tip: For every trip on a single day, we only care about the trip with the longest distance.

>Command:
```sql
SELECT DATE(lpep_pickup_datetime),sum(trip_distance) AS total
FROM green
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY total DESC
LIMIT 1
```
>Answer:
```
2019-09-26
```
## Question 5.Three biggest pick up Boroughs

Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown

Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?

>Command:
```sql
SELECT zone."Borough", SUM(green."total_amount") AS amount
FROM green
LEFT JOIN zone ON green."PULocationID" = zone."LocationID" 
WHERE zone."Borough" <> 'Unknown' AND DATE(green."lpep_pickup_datetime")='2019-09-18'
GROUP BY zone."Borough"
HAVING SUM(green."total_amount") > 50000;
```
>Answer:
```
"Brooklyn"	96333.23999999897
"Manhattan"	92271.29999999846
"Queens"	78671.70999999883
```
## Question 6. Largest tip

For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip? We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

>Command:
```sql
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
```
>**Answer :**
```
Astoria
```
# Terraform Terraform

## Question 7.  Creating Resources

After updating the main.tf and variable.tf files run:

```
terraform apply
```

> **Answer :**

```bash
(base) ola@oladeMacBook-Pro 1_terraform_gcp % terraform apply  

Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following symbols:

  + create

Terraform will perform the following actions:

google_bigquery_dataset.demo_dataset will be created

  + resource "google_bigquery_dataset" "demo_dataset" {
    + creation_time              = (known after apply)
    + dataset_id                 = "trips_data_all"
    + default_collation          = (known after apply)
    + delete_contents_on_destroy = false
    + effective_labels           = (known after apply)
    + etag                       = (known after apply)
    + id                         = (known after apply)
    + is_case_insensitive        = (known after apply)
    + last_modified_time         = (known after apply)
    + location                   = "US"
    + max_time_travel_hours      = (known after apply)
    + project                    = "coherent-ascent-379901"
    + self_link                  = (known after apply)
    + storage_billing_model      = (known after apply)
    + terraform_labels           = (known after apply)
      }

google_storage_bucket.demo-bucket will be created

  + resource "google_storage_bucket" "demo-bucket" {
    + effective_labels            = (known after apply)
    + force_destroy               = true
    + id                          = (known after apply)
    + location                    = "US"
    + name                        = "dtc_data_lake_coherent-ascent-379901"
    + project                     = (known after apply)
    + public_access_prevention    = (known after apply)
    + rpo                         = (known after apply)
    + self_link                   = (known after apply)
    + storage_class               = "STANDARD"
    + terraform_labels            = (known after apply)
    + uniform_bucket_level_access = (known after apply)
    + url                         = (known after apply)

    + lifecycle_rule {
      + action {
        + type = "AbortIncompleteMultipartUpload"
          }
      + condition {
        + age                   = 2
        + matches_prefix        = []
        + matches_storage_class = []
        + matches_suffix        = []
        + with_state            = (known after apply)
          }
          }
          }

Plan: 2 to add, 0 to change, 0 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes

google_bigquery_dataset.demo_dataset: Creating...
google_storage_bucket.demo-bucket: Creating...
google_bigquery_dataset.demo_dataset: Creation complete after 6s [id=projects/coherent-ascent-379901/datasets/trips_data_all]
google_storage_bucket.demo-bucket: Still creating... [10s elapsed]
google_storage_bucket.demo-bucket: Creation complete after 20s [id=dtc_data_lake_coherent-ascent-379901]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
```





# Submitting the solutions 

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw01
- You can submit your homework multiple times. In this case, only the last submission will be used.

Deadline: 29 January, 23:00 CET
