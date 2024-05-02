#!/bin/bash

# Define the base URL and pipeline run template
base_url="http://localhost:6789/api/pipeline_schedules/17/pipeline_runs/dd9d0f7f95474dd1aaab9f8d0cd6ff0c"
template='
{
  "pipeline_run": {
    "variables": {
      "year": "%d",
      "month": "%02d",
      "output_parquet": "nyc_collisions_%d_%02d_.parquet"
    }
  }
}'

# Loop through years from 2013 to 2023
for year in {2013..2023}; do
    # Loop through all 12 months
    for month in {1..12}; do
        # Execute curl command
        curl -X POST $base_url --header 'Content-Type: application/json' --data "$(printf "$template" $year $month $year $month)"
        # Sleep for 30 seconds
        sleep 30
    done
    echo ""
    echo "Finished year $year"
    echo ""
done

# # For 2024, loop through months from January to March (inclusive)
for month in {1..3}; do
    # Execute curl command
    curl -X POST $base_url --header 'Content-Type: application/json' --data "$(printf "$template" 2024 $month 2024 $month)"
    # Sleep for 30 seconds
    sleep 30
done
echo ""
echo "Finished year 2024"