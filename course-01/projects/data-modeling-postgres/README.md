# Data Modeling with Postgres Project

## Sparkify schema design and ETL pipeline
Sparkify, a music streaming startup, didn't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project consisted in the design and implementation of a star schema in a postgres database to enable analysis on the startup's data. The star schema consists of one fact table for songplay data referencing the following dimensions: user, songs, artists and time.

The data mart was modeled using a star schema since these schemas are denormalized, which results in different benefits for the analysis including:
* Simpler queries
* Query performance improvement
* Fast aggregations

Firstly, the ETL process carried out processes all of the files related to songs and artists. After this informations is loaded in the tables for their respective dimensions the log file is processed. This is done in this order since the log files contain the rest of the information required to create the remaining dimensions and the fact table. This ETL process fulfills the requirements of the project and enables the creation of the proposed schema in an efficient way.

## Project files
The files used for the project were:
* `sql_queries.py`: Contains the queries used to create and drop tables as well as the queries used to insert and find items.
* `create_tables.py`: Contains a script that drops (if exists) and creates the sparkify database. 
* `etl.ipynb`: Contains the preparation for the ETL pipeline. This file tests the different functions used to extract data from the json files, transform it to the appropriate formats and store them in the database.
* `test.ipynb`: Contains sample queries to check the contents of the database.
* `etl.py`: Contains the script used for the ETL pipeline. It uses the functions tested in the etl notebook for all the files in the dataset.