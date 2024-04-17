#!/bin/bash
sudo apt-get update
sudo apt-get install -y python3-pip
pip3 install astral > /var/log/install_astral.log 2>&1
# Make sure the Google Cloud SDK is installed and gsutil is available
gsutil cp /var/log/install_astral.log gs://collisions-first-try/spark_code/install_astral.log
