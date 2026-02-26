select count(distinct PULocationID) from `de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata_2024`;

select count(distinct PULocationID) from `de-zoomcamp-terraform-demo.nyc_taxi.yellow_tripdata_2024_non_partitioned`;

select count(*) from `de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata_2024`
where fare_amount = 0;

-- Create partitioned and clustered table
CREATE OR REPLACE TABLE de-zoomcamp-terraform-demo.nyc_taxi.yellow_tripdata_2024_partitioned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata_2024;


-- Query scans 310.24 MB
SELECT distinct VendorID 
FROM de-zoomcamp-terraform-demo.nyc_taxi.yellow_tripdata_2024_non_partitioned
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- Query scans 26.84 MB
SELECT distinct VendorID
FROM de-zoomcamp-terraform-demo.nyc_taxi.yellow_tripdata_2024_partitioned_clustered
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- Query scans 0 MB
select count(*) from de-zoomcamp-terraform-demo.nyc_taxi.yellow_tripdata_2024_partitioned_clustered;