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





