-- Docs: https://docs.mage.ai/guides/sql-blocks
CREATE OR REPLACE EXTERNAL TABLE `nyc-auto-accidents.nyc_collisions.spark_crash_external_data`
OPTIONS (
    format = 'parquet',
    uris = ['gs://collisions-first-try/spark_transformed_data/*.parquet']
);-- Docs: https://docs.mage.ai/guides/sql-blocks

