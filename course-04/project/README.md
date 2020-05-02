# Data Pipelines with Airflow

## Sparkify schema design and ETL pipeline
Sparkify, a music streaming startup, didn't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. Also, they have decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

This project consisted in the creation of high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to. This project will allow their analytics team to continue finding insights in what songs their users are listening to.

## Project files
The files used for the project were:
* `dl.cfg`: Contains the configurations to connect to the AWS resources.
* `etl.py`: Contains the script used for the ETL pipeline. It carries out the ETL process outlined previously.

## How to run
1. Edit the `dl.cfg` file to include the details of your AWS account.
2. Edit the `etl.py` file to include the information of the URLs for the input and output S3 buckets.
3. Run the `etl.py` script to run the ETL pipeline. This will extract the information from the input S3 buckets, use it to create the fact and dimension tables and then store them to the output S3 bucket in parquet files.