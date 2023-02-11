# Create Tables
External:
CREATE OR REPLACE EXTERNAL TABLE `de_zoomcamp_2023.external_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://de_zoomcamp_2023/data/fhv/fhv_tripdata_2019-*.csv.gz']
);

BQ Table:
CREATE OR REPLACE TABLE de_zoomcamp_2023.fhv_tripdata AS
SELECT * FROM de_zoomcamp_2023.external_fhv_tripdata;

# Question 1:
SELECT COUNT(1) FROM `dtc-de-375704.de_zoomcamp_2023.external_fhv_tripdata`;
Answer: 43,244,696

# Question 2:
SELECT COUNT(DISTINCT Affiliated_base_number) FROM `dtc-de-375704.de_zoomcamp_2023.external_fhv_tripdata`;
SELECT COUNT(DISTINCT Affiliated_base_number) FROM `dtc-de-375704.de_zoomcamp_2023.fhv_tripdata`;
Answer: 0 MB for the External Table and 317.94MB for the BQ Table

# Question 3:
SELECT COUNT (1) FROM `dtc-de-375704.de_zoomcamp_2023.external_fhv_tripdata` WHERE PUlocationID IS NULL AND DOlocationID IS NULL;
Answer: 717,748

# Question 4:
Answer: Partition by pickup_datetime Cluster on affiliated_base_number

# Question 5:
CREATE OR REPLACE TABLE dtc-de-375704.de_zoomcamp_2023.fhv_tripdata_partitoned
PARTITION BY DATE(pickup_datetime) 
CLUSTER BY Affiliated_base_number AS
SELECT * FROM dtc-de-375704.de_zoomcamp_2023.fhv_tripdata;

SELECT COUNT (DISTINCT Affiliated_base_number) 
  FROM dtc-de-375704.de_zoomcamp_2023.fhv_tripdata 
 WHERE DATE(pickup_datetime) > "2019-02-28" AND DATE(pickup_datetime) < "2019-04-01";

 SELECT COUNT(DISTINCT Affiliated_base_number) 
   FROM `dtc-de-375704.de_zoomcamp_2023.fhv_tripdata_partitoned_clustered` 
  WHERE DATE(pickup_datetime) > "2019-02-28" AND DATE(pickup_datetime) < "2019-04-01";

# Question 6.
Answer: GCP Bucket

# Question 7.
Answer: False