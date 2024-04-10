-- Docs: https://docs.mage.ai/guides/sql-blocks
CREATE OR REPLACE EXTERNAL TABLE `nyc-auto-accidents.nyc_collisions.external_data`
OPTIONS (
    format = 'parquet',
    uris = ['gs://collisions-first-try/ny_collision_data/*.parquet']
);
