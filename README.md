# New York City Taxi Data Project
Data engineering project working with New York City taxi data (inspired by DataTalksClub- Data Engineering Course)


# Description

This is a simple project which takes data from https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page, transforms it in order to visualize data extracted from this dataset. For simplicity sake, the taxi trip data used for this project only covers from January 1, 2019 to December 31, 2020. 


# Setup
I will be using Docker to containerize and run Airflow, etc.

- Built the infrastructure of Google Cloud Platform using Terraform.
- Built the DAGs to ingest data and upload it to Google Cloud Storage (Data Lake) as a parquet file
- Used Airflow to manage the workflows. 


