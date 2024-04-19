-- Docs: https://docs.mage.ai/guides/sql-blocks
CREATE OR REPLACE EXTERNAL TABLE `nyc-auto-accidents.nyc_collisions.weather_data_external`
OPTIONS (
    format = 'parquet',
    uris = ['gs://collisions-first-try/weather_data.parquet']
);-- Docs: https://docs.mage.ai/guides/sql-blocks
