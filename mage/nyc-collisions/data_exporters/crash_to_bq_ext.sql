CREATE OR REPLACE EXTERNAL TABLE `nyc-auto-accidents.nyc_collisions.crash_data_external`
OPTIONS (
    format = 'parquet',
    uris = ['gs://collisions-first-try/crash_data_spark_trans/*.parquet']
);

