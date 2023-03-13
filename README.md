# Corteva ETL challenge


This repository contains my solution to the challenge posed by Corteva. In this exercise, a dataset containing weather and crop yield data. In this exercise, I designed a database for maintaining these two datasets in two tables- Weather and Yield data. By using the weather data as source I designed another table Weather_stats that contains the statstics of weather for each year and each station ID.


## About Data

#### Weather Data Description


The wx_data directory [Link](https://github.com/maddy3940/corteva-code-challenge/tree/main/wx_data) has files containing weather data records from 1985-01-01 to 2014-12-31. Each file corresponds to a particular weather station from Nebraska, Iowa, Illinois, Indiana, or Ohio.

Each line in the file contains 4 records separated by tabs: 

1. The date (YYYYMMDD format)
2. The maximum temperature for that day (in tenths of a degree Celsius)
3. The minimum temperature for that day (in tenths of a degree Celsius)
4. The amount of precipitation for that day (in tenths of a millimeter)

Missing values are indicated by the value -9999.



#### Yield Data Description 

The yld_data directory [Link](https://github.com/maddy3940/corteva-code-challenge/tree/main/yld_data) has files containing yield data record from 1985 to 2014. 

Each line in the file contains 2 records separated by tabs: 

1. Year (YYYYMMDD format)
2. Yield

## Pipeline Architecture

![image](https://user-images.githubusercontent.com/44323045/224803035-91d1689a-6a7c-410e-afd3-e5ef03a4c806.png)

Ingestion pipeline Diagram


![image](https://user-images.githubusercontent.com/44323045/224805923-ff17c8f6-a3c2-43b9-8e2c-88f4c939bc32.png)

Data Analysis Pipeline

## ETL Procedure
1. Establish a connection with MySQL database running on port 3306
2. Check the last commit time when the pipeline was run. I have placed a file inside the Logs folder called commit_time.txt. It contains two rows that have the timestamp of the last commit that was made to the source data repo [Link](https://github.com/corteva/code-challenge-template). Let's call this committed time as old commit time

![image](https://user-images.githubusercontent.com/44323045/224821819-74dc5d91-47b8-4ba1-bdc4-2a97aae7c2a6.png)

3. When the pipeline runs I have written a decorator to get the latest commit time for the source data repo. If the new commit time is greater than the old commit time then it means the data has been updated and further functions will be executed. This step happens for both the Yield and Weather tables. This is added to avoid unnecessary pipeline runs.
4. If a new commit is made then first the source data is cloned into a temporary folder in the current working directory. (This step is not optimized, we could have optimized it if it was an S3 bucket by only getting the new files that were inserted, but here I am getting the whole repo. I tried to use the timestamp to just get the new data but there are limitations on the number of requests made via Github API)
5. Now inside the temp folder, first all the files for weather inside wx_data are read into a data frame and a hash value is computed which is nothing but a concatenation of all the row values. This is done to uniquely identify each row and eliminate duplicates.
6. Then there is a change detection logic (CDC) in place to get rid of duplicate data or data that has already been entered into the database. Here we have two cases if the SQL database is empty or it already contains some data from the previous run. Both cases are being handled. New data that is coming may have been already inserted in the database. To avoid this we are using hash values and in the CDC step we are computing a set difference between the newly arrived data and the data that is already present in the SQL database on the hash value. Then we drop the duplicates. By doing this we only preserve the unique new data that has arrived in the latest pipeline run.
7. Here we insert the row into the table even if one column of data has changed, i.e say for example when running the pipeline on 10th March 2023 we got some value for station id 1 for that day. Now when we again run the pipeline on 11th March 2023, and a new data point arrives having a timestamp of 10th March 2023, but with different temperature values, we insert it into the weather table. If all the column reading is the same then we do not insert that record.   
8. Once the new data is derived, we insert it into the database and commit the transaction. We have placed two decorators to count the count of data being inserted and the amount of time it requires for the execution of all the above-mentioned functions. All these logs are logged into the execution_logs.log file in the current working directory. 
9. The same steps from 4 to 7 are repeated for yield data.
10. If the pipeline is being run for the first time, a new database, as well as the tables, are automatically created.
11. Once weather and yield data is inserted into the tables, the function compute_and_store_analysis function is run to compute the new statistics for the new data that has arrived. We only run the compute_and_store_analysis if some new weather data has arrived. The average maximum temperature, Average minimum temperature, and Total accumulated precipitation for each year and each station id are computed.
12. Here we have two cases- 
  a) When the pipeline is run for the first time or data for a new station id or year comes in then all the data is considered and new statistics for every year are calculated and inserted into the database into the weather_stats table 
  b) - When data is already present in the SQL database then we proceed differently. For example, suppose we have weather data from 1st Jan to 31st July 2022 initially. During the first run, the average and sum statistics are calculated without any issues. But then suppose during the second run data from 1st Aug to 31st Dec comes in. Now we have to recalculate the average and totals for the year 2022. 
  - Now since we have partial data in the SQL database and partial data we got during the latest pipeline run, we use the hash value column for the weather stats table to get the partial data from the SQL database. For weather stats, hash_value is the concatenation of station id and year which is used to uniquely identify each row in the weather_stats table. 
  - We now have the full data for 2022 and now we compare the statistics. But before inserting it into the weather_stats table we first delete the existing stats record from the weather stats table, since a particular weather station and a year cannot have multiple statistics. After deleting we insert the newly compted statistics.
13. Logs of the run will be appended to a log file which you can access later.

## Flask Application

- I have build simple flask application with two endpoints to get the weather and weather statistics data
- If you run the docker contianer the endpoints will be active for you to access
- I have also added a Swagger API endpoint to provide documentation at the endpoint- /apidocs
 

## Results of pipeline run-

I ran the pipleine fully and exported the data from the sql table. Here is the data obtained after initial run- 

https://drive.google.com/drive/folders/1NJfuJmVlmHczBMKu5S5_2FNocy6lkkum?usp=sharing 

## How to reporduce results

#### Local Run via jupyter notebook
- Clone the repository
- Run a Mysql service on the port 3306.
- Edit the config.json file inside Local_run folder to reflrect your database credentials
- Make sure you have all the required packages mentioned in Local_run/requirements.txt file and python version 3.7.1 along with jupyter notebook.
- Run all the cells in the ETL.ipynb notebook
- Open the mysql database and check the three tables that are created.
- You can run the app.py Flask app locally to get the data from rest API endpoints- /api/weather, /api/weather/stats

For the notebook to run quickly I am taking a subset of the data to insert into the database-
![image](https://user-images.githubusercontent.com/44323045/224834987-dd00f3d6-ddf8-4ef4-8932-4b5d49d9e410.png)

Remove the slicing ([:10]) to insert all the records into the databsase


#### Run as Docker services
- Install docker
- Build two Docker images. One inside the folder Ingestion and the Second one inside Flask App using the commands-

```python
docker build -t corteva_ingestion_etl .
docker build -t corteva_flask_app .
```

- Inside the etldocker folder edit the docker-compose.yml file 
![image](https://user-images.githubusercontent.com/44323045/224836903-cb3d510a-64ef-4d57-aa93-f75fb73e0f0c.png)

I am mounting the Files directory for Docker container to access at location /app. Docker will be able to access the config.json file and write the logsat this location. Edit the highlighted location to point to the Files folder inside etldocker folder.

- Make sure the post 3306 is free. Kill any tasks if its not free
- Run the following command from the etldocker directory 
```python
docker-compose up
```
![image](https://user-images.githubusercontent.com/44323045/224838992-6da5497a-1ef6-4636-90b8-0a23dcd97af2.png)

- Now if you see the Docker Desktop app three services will be active- web, ingestion and mysqldb.
- Ingestion pipleine will run and stop after its done writing the data in the sqldb
- You can access these tables using MySQL workbench once the ingestion pipleine is run
![image](https://user-images.githubusercontent.com/44323045/224839317-2bc89385-9c74-42f2-beae-430a5ae4a7e1.png)

- You can also access the data using Flask web app from the endpoints- /api/weather, /api/weather/stats


## Output schema
- Weather
![image](https://user-images.githubusercontent.com/44323045/224839555-bdce9465-daed-4870-b821-f1776030889c.png)

- Yield
![image](https://user-images.githubusercontent.com/44323045/224839618-bfda681c-6eb8-45a6-a7fd-7f3fddf5091d.png)

- Weather_stats
![image](https://user-images.githubusercontent.com/44323045/224839730-ad823f79-001a-4dec-866c-52e017270455.png)

![image](https://user-images.githubusercontent.com/44323045/224840439-09a76295-c97b-4ab1-9dd4-798476c69a7c.png)

Foreign Key constaints can be added here between Station_ID and Year between all three tables


## More enhancements

- I did not try to change the data. I am inserting the data as it is to the database. Depending upon the applcation of this data need might arise to drill down on the day or month or year. If thats the case then while insertion we could have taken the date column and created new columns like month, or year
- I did not get time to write test cases and perfrom unit testing. But during development I made sure to handle all ther errors and configre proper logging.
- If I had more time I wa splanning to set up a Github Actions workflow to run the pipleine on a schedule on Heroku. I was planning to push the docker images to docker hub, configure heroku to run the services on a schedule so that whenever new data arrives the pipeline will be triggered.





