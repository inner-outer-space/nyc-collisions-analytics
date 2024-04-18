CREATE OR REPLACE EXTERNAL TABLE `nyc-auto-accidents.nyc_collisions.crash_external_data`
OPTIONS (
    format = 'parquet',
    uris = ['gs://collisions-first-try/spark_transformed_data/*.parquet']
);

