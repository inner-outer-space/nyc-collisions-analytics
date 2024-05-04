CREATE OR REPLACE EXTERNAL TABLE `{{ google_project_id }}.{{ google_bq_dataset }}.crash_data_external`
OPTIONS (
    format = 'parquet',
    uris = ['gs://{{ google_bucket }}/{{ google_gcs_spark }}/{{ output_file_name }}']
    --uris = ['gs://{{ google_bucket }}/{{ google_gcs_spark }}/*.parquet']
);

-- Docs: https://docs.mage.ai/guides/sql-blocks
