# Data Engineering Project #1 : Fitpipe, a hourly max heart rate ETL pipeline 
My first attempt at a rough ETL pipeline; technologies include spark, GCS, prefect orchestration, and terraform

## Architecture
**Infrastructure*:** Terraform is used for 'infrastructure as code' to set up a data lake in GCS
**Orchestration/Scheduling:** prefect is used to perform the DAG flow batch-processing

**Dag flow:** The following DAG process is run once an hour through prefects scheduler/ orion
1. *gcs_pull_task:* pull from GCS a copy of my pre-uploaded fitbit data in csv format, save locally for appending
2. *fitbit_task* Pull in data from my personal fitbit using the fitbit API from the last hour, transform to a pandas dataframe, and return it
3. *pyspark_task*: use pyspark/SQL to transform the data and pull the largest rate for the hour
4. *overwrite_master_table*: append the new fitibit data to the csv file
5. *gcs_push_task*: push the updated csv file to GCS and overwrite the old version


## Dashboard:
The dashboard was built using google datastudio. It shows the max heart rate over time, the average max heart rate, and the current max of the max heart rates.


# Requierments:
I was able to install most of the requierments through guides in the data engineering zoomcamp course, or through online articles. Thanks to everyone who helped with their public repos/ articles!
1. **Terraform/GCP Cloud Platform:** [DE zoomcamp guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_1_basics_n_setup/1_terraform_gcp)
2. **Prefect:** [prefect install documentation](https://docs.prefect.io/getting-started/installation/), and [prefect beginners guide](https://docs.prefect.io/tutorials/first-steps/)
4. **Fitbit API:** [ Fitbit API article by Stephen Hsu](https://towardsdatascience.com/collect-your-own-fitbit-data-with-python-ff145fa10873)
5. **Spark/PySpark:** DE zoomcamp [spark windows/linux/macOS download guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_5_batch_processing/setup), and [Pyspark download guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_5_batch_processing/setup/pyspark.md)

 
 # Inspirations:
 Here are a few repos and projects by others that I looked at for inspiration and help with project repo formatting -- thank you to the creators.
1. [ris-tlp adiophile-e2e-pipeline](https://github.com/ris-tlp/audiophile-e2e-pipeline)
2. [MarcosMJD Global Historical Climatology Pipeline](https://github.com/MarcosMJD/ghcn-d)
