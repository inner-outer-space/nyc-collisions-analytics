-- Docs: https://docs.mage.ai/guides/sql-blocks
CREATE OR REPLACE EXTERNAL TABLE `{{ google_project_id }}.{{ google_bq_dataset }}.external_weather_data`
OPTIONS (
    format = 'parquet',
    uris = ['gs://{{ google_bucket }}/weather_data.parquet']
);-- Docs: https://docs.mage.ai/guides/sql-blocks
