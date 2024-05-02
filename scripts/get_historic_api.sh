#!/bin/bash
# This script is used to trigger the pipeline that extracts NYC Collisions records 
# from NYC Open Data API from Jan of start_year to Dec of end_year.
# Arguments: 
#   start_year and end_year.
# Output: 
#   files are saved in parquet format to a GCP bucket.

# Check if start and end years are provided as arguments
if [ $# -ne 2 ]; then
    echo "Usage: $0 <start_year> <end_year>"
    exit 1
fi

# Assign start and end years from arguments
start_year=$1
end_year=$2

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

# Loop through years from start_year to end_year
for ((year=start_year; year<=end_year; year++)); do
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
# for month in {1..3}; do
#     # Execute curl command
#     curl -X POST $base_url --header 'Content-Type: application/json' --data "$(printf "$template" 2024 $month 2024 $month)"
#     # Sleep for 30 seconds
#     sleep 30
# done
# echo ""
# echo "Finished year 2024"