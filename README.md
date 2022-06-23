# New York City Taxi Data Project
Data engineering project working with New York City taxi data (inspired by DataTalksClub- Data Engineering Course)


# Description

This is a simple project which takes data from https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page, transforms it in order to visualize data extracted from this dataset. For simplicity sake, the taxi trip data used for this project only covers from January 1, 2019 to December 31, 2020. 

# Dataset

For this project, two sets of datasets which represent the green and yellow taxi trip data were gathered from https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page. As the site explains, the data include fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts.

# Dashboard

Google Data Studio was used for the visualizations. 
1. First visualization contains a brief trip analysis data. It can be accessed [here](https://datastudio.google.com/s/gBXL91OlhGo).
2. Second visualization contains a brief revenue report on the taxi data. It can be accessed [here](https://datastudio.google.com/s/s_HgmTgDn0w).

# Project Details and Implementation
This project uses Google Cloud Platform, specifically Cloud Storage (as a Data Lake) and BigQuery. Docker was used to containerize and run Airflow.

Terraform was used to managed the infrastructure of Google Cloud Platform.

Data ingestion is carried out by Airflow DAGs. DAGs then upload the data to Google Cloud Storage (GCS) as a parquet file. Once the data is in GCS, the DAG rearrange the files in more organized folders. At the end of the DAG, an external table is created in BigQuery.

dbt was used to define the data warehouse and for creating the transformations needed to create the two visualizations for the dashboard (see above). Multiple views are created in a staging phase and two final tables containing details of all trips (for both green and yellow taxi) and a revenue table are materialized in the deployment phase.
    - ![image](https://user-images.githubusercontent.com/101911504/174879562-7bc14012-5348-4017-932f-73ed7de561dc.png)

Note: FHV data is currently stored in GCS but not translated into the data warehouse due to an unexpected error. More work is being done to fix this issue.

