CREATE OR REPLACE EXTERNAL TABLE `{{ google_project_id }}.{{ google_bq_dataset }}.crash_data_external`
OPTIONS (
    format = 'parquet',
    uris = ['gs://{{ google_bucket }}/{{ google_gcs_spark }}/collisions_batch_0.parquet']
    --uris = ['gs://{{ google_bucket }}/{{ google_gcs_spark }}/*.parquet']
);

