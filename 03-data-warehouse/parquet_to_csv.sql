CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de_zoomcamp_03_nyc_taxi/yellow/yellow_tripdata_2020-01.parquet']
);

EXPORT DATA OPTIONS (
  uri = 'gs://de_zoomcamp_03_nyc_taxi_csv/yellow/yellow_tripdata_2020-01_*.csv',
  format = 'CSV',
  overwrite = true,
  header = true
)
AS
SELECT
  *
FROM `de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata`;


CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de_zoomcamp_03_nyc_taxi/yellow/yellow_tripdata_2020-02.parquet']
);

EXPORT DATA OPTIONS (
  uri = 'gs://de_zoomcamp_03_nyc_taxi_csv/yellow/yellow_tripdata_2020-02_*.csv',
  format = 'CSV',
  overwrite = true,
  header = true
)
AS
SELECT
  *
FROM `de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata`;


CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de_zoomcamp_03_nyc_taxi/yellow/yellow_tripdata_2020-03.parquet']
);

EXPORT DATA OPTIONS (
  uri = 'gs://de_zoomcamp_03_nyc_taxi_csv/yellow/yellow_tripdata_2020-03_*.csv',
  format = 'CSV',
  overwrite = true,
  header = true
)
AS
SELECT
  *
FROM `de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata`;

CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de_zoomcamp_03_nyc_taxi/yellow/yellow_tripdata_2020-04.parquet']
);

EXPORT DATA OPTIONS (
  uri = 'gs://de_zoomcamp_03_nyc_taxi_csv/yellow/yellow_tripdata_2020-04_*.csv',
  format = 'CSV',
  overwrite = true,
  header = true
)
AS
SELECT
  *
FROM `de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata`;

CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de_zoomcamp_03_nyc_taxi/yellow/yellow_tripdata_2020-05.parquet']
);

EXPORT DATA OPTIONS (
  uri = 'gs://de_zoomcamp_03_nyc_taxi_csv/yellow/yellow_tripdata_2020-05_*.csv',
  format = 'CSV',
  overwrite = true,
  header = true
)
AS
SELECT
  *
FROM `de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata`;

CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de_zoomcamp_03_nyc_taxi/yellow/yellow_tripdata_2020-06.parquet']
);

EXPORT DATA OPTIONS (
  uri = 'gs://de_zoomcamp_03_nyc_taxi_csv/yellow/yellow_tripdata_2020-06_*.csv',
  format = 'CSV',
  overwrite = true,
  header = true
)
AS
SELECT
  *
FROM `de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata`;

CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de_zoomcamp_03_nyc_taxi/yellow/yellow_tripdata_2020-07.parquet']
);

EXPORT DATA OPTIONS (
  uri = 'gs://de_zoomcamp_03_nyc_taxi_csv/yellow/yellow_tripdata_2020-07_*.csv',
  format = 'CSV',
  overwrite = true,
  header = true
)
AS
SELECT
  *
FROM `de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata`;

CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de_zoomcamp_03_nyc_taxi/yellow/yellow_tripdata_2020-08.parquet']
);

EXPORT DATA OPTIONS (
  uri = 'gs://de_zoomcamp_03_nyc_taxi_csv/yellow/yellow_tripdata_2020-08_*.csv',
  format = 'CSV',
  overwrite = true,
  header = true
)
AS
SELECT
  *
FROM `de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata`;


CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de_zoomcamp_03_nyc_taxi/yellow/yellow_tripdata_2020-09.parquet']
);

EXPORT DATA OPTIONS (
  uri = 'gs://de_zoomcamp_03_nyc_taxi_csv/yellow/yellow_tripdata_2020-09_*.csv',
  format = 'CSV',
  overwrite = true,
  header = true
)
AS
SELECT
  *
FROM `de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata`;


CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de_zoomcamp_03_nyc_taxi/yellow/yellow_tripdata_2020-10.parquet']
);

EXPORT DATA OPTIONS (
  uri = 'gs://de_zoomcamp_03_nyc_taxi_csv/yellow/yellow_tripdata_2020-10_*.csv',
  format = 'CSV',
  overwrite = true,
  header = true
)
AS
SELECT
  *
FROM `de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata`;

CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de_zoomcamp_03_nyc_taxi/yellow/yellow_tripdata_2020-11.parquet']
);

EXPORT DATA OPTIONS (
  uri = 'gs://de_zoomcamp_03_nyc_taxi_csv/yellow/yellow_tripdata_2020-11_*.csv',
  format = 'CSV',
  overwrite = true,
  header = true
)
AS
SELECT
  *
FROM `de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata`;

CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de_zoomcamp_03_nyc_taxi/yellow/yellow_tripdata_2020-12.parquet']
);

EXPORT DATA OPTIONS (
  uri = 'gs://de_zoomcamp_03_nyc_taxi_csv/yellow/yellow_tripdata_2020-12_*.csv',
  format = 'CSV',
  overwrite = true,
  header = true
)
AS
SELECT
  *
FROM `de-zoomcamp-terraform-demo.nyc_taxi.external_yellow_tripdata`;


