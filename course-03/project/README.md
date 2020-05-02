# ETL Pipeline with Spark, Elastic Map Reduce (EMR) and S3

## Sparkify schema design and ETL pipeline
Sparkify, a music streaming startup, didn't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project consisted in the construction of an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## Project files
The files used for the project were:
* `dl.cfg`: Contains the configurations to connect to the AWS resources.
* `etl.py`: Contains the script used for the ETL pipeline. It carries out the ETL process outlined previously.

## How to run
1. Edit the `dl.cfg` file to include the details of your AWS account.
2. Edit the `etl.py` file to include the information of the URLs for the input and output S3 buckets.
3. Run the `etl.py` script to run the ETL pipeline. This will extract the information from the input S3 buckets, use it to create the fact and dimension tables and then store them to the output S3 bucket in parquet files.