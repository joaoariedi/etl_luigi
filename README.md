# LUIGI and PostgreSQL ETL tests
Get data from an API using requests, process and send to a postgres table, all tasks scheduled using crontab.

![GitHub Logo](images/scheduler.png)

## configurations
File to place all the configuration params
## extract
Get data from API and store as a json file
## transform
Extract user_name and user_id from a json file and store as a parquet file
## load
Copy the data from parquet file to a local Postgres table
## main
Insert the volatile params and start internal tasks using a build function 