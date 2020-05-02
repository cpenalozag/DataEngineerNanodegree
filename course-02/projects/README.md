# Data Warehousing with Redshift and S3

## Sparkify schema design and ETL pipeline
Sparkify, a music streaming startup, didn't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project consisted in the construction of an ETL pipeline that extracts the data from S3 buckets, stages it in Redshift, and transforms data into a set of dimensional tables to facilitate further analysis. The dimensional model used is the one proposed for the previous project.

## Project files
The files used for the project were:
* `dwh.cfg`: Contains the configurations to connect to the S3 buckets with the raw files and to the Redshift cluster.
* `sql_queries.py`: Contains the queries used to create and drop tables as well as the queries used to insert items from the staging tables into the facts and dimensions.
* `create_tables.py`: Contains a script that drops (if exists) and creates the tables for staging and the dimensional model. 
* `etl.py`: Contains the script used for the ETL pipeline. It uses the functions tested in the etl notebook for all the files in the dataset.

## How to run
1. Edit the `dwh.cfg` file to include the details of your Redshift database and the IAM role attached to it.
2. Run the `create_tables.py` script to create the tables required for staging and the dimensional model.
3. Run the `etl.py` script to copy the data from the S3 buckets into the staging tables and to fill the dimensional model tables from the staging buckets.