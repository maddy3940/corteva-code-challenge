import pandas as pd
import requests
import mysql.connector
from sqlalchemy import create_engine
from datetime import datetime,timedelta
import pytz
import os
import warnings
import json
import logging
import time
from functools import wraps
from datetime import datetime
import inspect
import shutil
import subprocess
warnings.filterwarnings("ignore")


# Decorators

def execution_time_log():
    """Decorator to log time of execution of a function
    """
    def inner_decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                start_time = datetime.now()
                result = func(*args, **kwargs)
                end_time = datetime.now()
                execution_time = end_time - start_time
                logging.info(f"The function {func.__name__} - Ran for {execution_time} seconds")    
                return result
            except Exception as e:
                logging.info(f"The function {func.__name__} encountered an error:  {e}")  
        return wrapper
    return inner_decorator

def error_log(function_name):
    """Log errors

    Args:
        function_name (string): Function name to print in log if any error occurs
    """
    def inner_decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                logging.info(f"Function {function_name} encountered an error: {e}")  
        return wrapper
    return inner_decorator


def counts_log():
    """Log counts of records inserted in database
    """
    def inner_decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
                logging.info(f"The function {func.__name__}  inserted {len(result)} records")         
            except Exception as e:
                logging.info(f"The function {func.__name__} encountered an error:  {e}")  
        return wrapper
    return inner_decorator


def check_commit_time(file_name):
    """Check if new commit has been made to source repo. If yes then execute ingestion pipeline

    Args:
        file_name (string): Can be either Weather or Yield
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                try:
                    old_commit_times = pd.read_csv('/app/commit_logs/commit_time.txt',sep='\t')
                    old_commit_time = datetime.strptime(old_commit_times[old_commit_times['Table name']==file_name]['Commit_Time'].values[0], "%Y-%m-%d %H:%M:%S")
                except:
                    logging.info("Historical commit data does not exist. Adding new commit data...") 
                    old_commit_times = {"Table name":['Weather','Yield'],"Commit_Time":['2000-01-01 00:00:00','2000-01-01 00:00:00']}
                    old_commit_times = pd.DataFrame(old_commit_times)
                    old_commit_time = None
                new_commit_time = get_last_commit_time(repo_url,weather_path) if file_name=='Weather' else get_last_commit_time(repo_url,yield_path)
                new_commit_time = datetime.strptime(new_commit_time[:-6], "%Y-%m-%d %H:%M:%S") 
                
                # Check if new commit time is greater than old commit time
                if old_commit_time is None or new_commit_time > old_commit_time:
                    logging.info("A new commit has been made to source data. Executing ingestion pipeline...")
                    old_commit_times.loc[old_commit_times['Table name'] == file_name, 'Commit_Time'] = new_commit_time
                    res = func(*args, **kwargs)
                    old_commit_times.to_csv('/app/commit_logs/commit_time.txt', sep='\t', index=False)
                    return res
                    
                else:
                    logging.info("No new commit has been made to source data. No need to execute ingestion pipeline")
            except Exception as e:
                logging.info(f"The function {func.__name__} encountered an error:  {e}")              
                
        return wrapper
    return decorator



@check_commit_time('Weather')
def read_write_weather_cdc_data():
    """- Read weather data from source. 
    - Get cdc data i.e new data that is not present currently in SQL database
    - Write new weather data inside SQL 

    Returns:
        pandas Dataframe: Dataframe containing new data
    """
    logging.info("Entered read write weather function")
    df_weather = get_weather_data()
    weather_cdc = get_cdc_data('weather',cnx,cursor,df_weather)
    
    if len(weather_cdc)!=0:
        logging.info("Writing into Weather table")
        write_df_to_db(weather_cdc,'weather')
    return weather_cdc


@check_commit_time('Yield')
def read_write_yield_cdc_data():
    """- Read Yield data from source. 
    - Get cdc data i.e new data that is not present currently in SQL database
    - Write new Yield data inside SQL 

    Returns:
        pandas Dataframe: Dataframe containing new data
    """
    df_yield = get_yield_data()
    yield_cdc = get_cdc_data('yield',cnx,cursor,df_yield)
    
    if len(yield_cdc)!=0:
        logging.info("Writing into Yield table")
        write_df_to_db(yield_cdc,'yield')
    return yield_cdc


@execution_time_log()
def get_weather_data():
    """Read all weather data text files from current working directory

    Returns:
        pandas Dataframe: Dataframe containing weather data
    """
    logging.info("Run get_weather_data entered")
    df_list = []
    # Loop through the list of files and download and read each file into a dataframe
    temp_path=os.getcwd()+'/app/Temp/wx_data'
    for file_name in os.listdir(temp_path):        
        df = pd.read_csv(os.path.join(temp_path, file_name), sep='\t',names=['Date','Maximum_Temperature','Minimum_Temperature','Precipitation'])        
        df['Station_ID']=file_name[:-4]
        df_list.append(df)

    # Concatenate the dataframes into one
    df_all = pd.concat(df_list, axis=0, ignore_index=True)
    df_all['hash_value'] = df_all.apply(lambda row: ''.join(str(value) for value in row.values), axis=1)
    df_all['Date']=pd.to_datetime(df_all['Date'], format='%Y%m%d')
    logging.info("Success get_weather_data")
    return df_all

    

@execution_time_log()
def get_yield_data():
    """Read all yield data text files from current working directory

    Returns:
        pandas Dataframe: Dataframe containing yield data
    """
    # Yield
    # List to hold the dataframes
    df_yield_list = []
    temp_path=os.getcwd()+'/app/Temp/yld_data'
    for file_name in os.listdir(temp_path):
        df = pd.read_csv(os.path.join(temp_path, file_name), sep='\t',names=['Year','Yield'])
        df_yield_list.append(df)

    # Concatenate the dataframes into one
    df_yield_all = pd.concat(df_yield_list, axis=0, ignore_index=True)
    df_yield_all['hash_value'] = df_yield_all.apply(lambda row: ''.join(str(value) for value in row.values), axis=1)
    return df_yield_all    

@error_log('get_last_commit_time')
def get_last_commit_time(repo_url,folder_path):
    """Get timestamp of when data was last updated

    Args:
        repo_url (string): source repo url
        folder_path (string): sub dir of source data

    Returns:
        string: timestamp of last commit
    """
    path = "/commits?path="

    # Make the API request and get the response as a JSON object
    response = requests.get(repo_url + path+ folder_path)
    commits = response.json()

    # Get the timestamp of the latest commit that modified the file
    timestamp = commits[0]['commit']['committer']['date']

    utc_dt = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ')  # Convert UTC string to datetime object
    local_tz = pytz.timezone('US/Eastern')  # Specify the timezone you want to convert to
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)

    return str(local_dt)


@counts_log()
def write_df_to_db(df,table_name):
    """Write dataframe to My SQL database

    Args:
        df (pandas Dataframe): Dataframe to write
        table_name (string): Table name

    Returns:
        pandas Dataframe: Dataframe that was passed
    """
    df.to_sql(table_name,con=engine, if_exists='append', index=False)
    cursor.execute("COMMIT")
    return df


@execution_time_log()
def get_cdc_data(target_table,connection,cursor,new_df):
    """- Get existing hash_values from sql database
    - Compute hash_value of new data that was fetched
    - Get set difference to obtain new data
    - Return new data that needs to be added to the sql database

    Args:
        target_table (string): name of target table
        connection (connection): connection object
        cursor (cursor): _cursor object
        new_df (pandas dataframe): new dataframe

    Returns:
        pandas dataframe: new dataframe with hash values
    """
    subquery = f"SELECT hash_value FROM {target_table}"
    cursor.execute('USE corteva')
    try:
        existing_hashes = pd.read_sql(subquery, connection)
    except:
        logging.info(f"{target_table} table has not yet been created in the database. Creating {target_table} table..") 
        existing_hashes = pd.DataFrame(columns=['hash_value'])
    try:
        cdc_df= new_df[~new_df['hash_value'].isin(existing_hashes['hash_value'])]
        cdc_df.drop_duplicates(inplace=True)
        return cdc_df
    except:
        logging.info(f"Either all newly committed values already present in the table") 
        return pd.DataFrame(columns=new_df.columns.tolist()+['hash_value'])
    


@error_log('clean_df')
def clean_df(df):
    """Clean dataframe to filter out incorrect values

    Args:
        df (pandas dataframe): dataframe to clean

    Returns:
        pandas dataframe: Clean dataframe
    """
    df = df.loc[(df['Maximum_Temperature']!=-9999) & (df['Minimum_Temperature']!=-9999) & (df['Precipitation']!=-9999)]
    df['Year']= df['Date'].dt.year
    df['hash_value']=df['Year'].astype(str)+df['Station_ID']
    return df[['hash_value','Station_ID','Year','Maximum_Temperature','Minimum_Temperature','Precipitation']]


@error_log('get_year_station_id')
def get_year_station_id(df):
    """Get distinct has values of dataframe

    Args:
        df (pandas Dataframe): Dataframe

    Returns:
        pandas Dataframe: Pandas dataframe with only unique hash_value
    """
    distinct_hash = df['hash_value'].unique()
    return distinct_hash


@error_log('get_existing_year_station_id')
def get_existing_year_station_id():
    """Get existing hash_value from sql database

    Returns:
        pandas Dataframe: Hash_value of existing data
    """
    query_hash = f"SELECT distinct concat(year(Date),Station_ID) as hash_value FROM weather"
    cursor.execute('USE corteva')
    try:
        existing_hash = pd.read_sql(query_year, cnx)
        return existing_hash
    except:
        return pd.DataFrame(columns=['hash_value'])



@error_log('intersection_year_stationid_new_old')
def intersection_year_stationid_new_old(df): 
    """Set difference of hash values of old and new data

    Args:
        df (pandas dataframe): Dataframe containing new data

    Returns:
        list: List of hash values
    """
    new_hash = get_year_station_id(df)
    
    existing_hash =  get_existing_year_station_id()
    existing_hash= existing_hash['hash_value'].tolist()
    
    stats_hash_to_compute = [x for x in new_hash if x not in existing_hash]

    return list(set(stats_hash_to_compute))


@error_log('compute_new_stats')
def compute_new_stats(df):
    """Compute stats

    Args:
        df (pandas dataframe): Dataframe object for which we need to compute stats

    Returns:
        pandas Dataframe: Data frame object with stats computed
    """
    # Group the data by Station_ID and Year
    grouped = df.groupby(['Station_ID', 'Year'])
    
    # Calculate the average of Maximum_Temperature, Minimum_Temperature, and Precipitation for each group
    agg_df = grouped.agg({'Maximum_Temperature': 'mean',
                          'Minimum_Temperature': 'mean',
                          'Precipitation': 'sum'})

    # Rename the columns to match the desired output schema
    agg_df = agg_df.rename(columns={'Maximum_Temperature': 'Average maximum temperature',
                                     'Minimum_Temperature': 'Average minimum temperature',
                                     'Precipitation': 'Total accumulated precipitation'})
    
    # Reset the index to make Station_ID and Year columns
    agg_df = agg_df.reset_index()

    agg_df['hash_value']=agg_df['Station_ID']+ agg_df['Year'].astype('str')

    return agg_df



@error_log('delete_old_inset_new_stats')
def delete_old_insert_new_stats(weather_df_stats,stats_hash_to_compute):
    """Delete old stats from weather stats table

    Args:
        weather_df_stats (pandas dataframe): Dataframe object with new stats
        stats_hash_to_compute (list): hash_values for which we need to delete old stats from weather_stats and insert new stats

    Returns:
        pandas Dataframe: Data frame object with stats computed
    """
    
    
    try:
        for i in range(len(stats_hash_to_compute)):
            query = f"""DELETE FROM Weather_Stats WHERE Year = '{stats_hash_to_compute[i][0]}' and Station_ID = '{stats_hash_to_compute[i][1]}'"""
            cursor.execute(query)
            cursor.execute("COMMIT")
    except Exception as e:
        logging.info("Weather_Stats table has not yet been created in the database. Creating Weather_Stats table..")
    logging.info("Writing into Weather Stats table")
    write_df_to_db(weather_df_stats,'Weather_Stats')
    


@error_log('filter_by_year_stationid')
def filter_by_year_stationid(df,stats_hash_to_compute):
    """Get weather data from sql table for particular year and station id for which we need to compute stats 

    Args:
        df (pandas dataframe): New cdc Dataframe object with weather
        stats_hash_to_compute (list): hash_values for which we need to delete old stats from weather_stats and insert new stats

    Returns:
        pandas Dataframe: Data frame object using which we will compute stats
    """
    
    df_old=[]
    for i in range(len(stats_hash_to_compute)):
        query = f"""
        SELECT Station_ID,year(date) as Year,Maximum_Temperature,Minimum_Temperature,Precipitation
        FROM weather
        WHERE year(date) ={str(stats_hash_to_compute[i][0])} AND Station_ID ='{stats_hash_to_compute[i][1]}'"""
        
        # Execute SQL query with parameters
        df_old_temp = pd.read_sql(query, con=cnx)
        df_old.append(df_old_temp)

    try:
        df_old = pd.concat(df_old, axis=0, ignore_index=True)
    except:
        df_old =  pd.DataFrame(columns=['Station_ID','Year','Maximum_Temperature','Minimum_Temperature','Precipitation'])
    new_df = pd.concat([df[['Station_ID','Year','Maximum_Temperature','Minimum_Temperature','Precipitation']], df_old])

    return new_df


@error_log('compute_and_store_analysis')
def compute_and_store_analysis(weather_df):
    """-Clean dataframe, get set difference of new hash_values, get old data, compute new stats, delete old stats from table and insert new stats

    Args:
        weather_df (_type_): _description_
    """
    weather_df_clean = clean_df(weather_df)
    stats_hash_to_compute = intersection_year_stationid_new_old(weather_df_clean)
    stats_hash_to_compute = [(x[:4], x[4:]) for x in stats_hash_to_compute]
    weather_df_clean_filtered = filter_by_year_stationid(weather_df_clean,stats_hash_to_compute)
    weather_df_stats = compute_new_stats(weather_df_clean_filtered)
    delete_old_insert_new_stats(weather_df_stats,stats_hash_to_compute)

try:
    config_file = os.path.join('/app', 'config.json')
    with open(config_file, 'r') as f:
        config = json.load(f)
    # Variables
    repo_url = config['repo_url']
    weather_path = config['weather_path']
    yield_path = config['yield_path']
    weather_api_endpoint = config['weather_api_endpoint']
    yield_api_endpoint = config['yield_api_endpoint']
    url = config['url']
    
except Exception as e:
    logging.info(f"Some error when loading or parsing config file {e}")

# Connection
# cnx = mysql.connector.connect(
#     host=config['database']['host'],
#     user=config['database']['username'],
#     password=config['database']['password'],
#     port=config['database']['port']
# )
try:
    cnx = mysql.connector.connect(
        host="mysql_db",
        user="root",
        password="password",
        port=3306
    )
    cursor = cnx.cursor()
    cursor.execute('CREATE DATABASE IF NOT EXISTS corteva ')
    engine = create_engine('mysql+mysqlconnector://root:password@mysql_db:3306/corteva')
    
except Exception as e:
    logging.info(f"Issue while connecting to SQL server {e}")

def run_main():  
    # Inestion
    logging.info("Entered main function")
    weather_df = read_write_weather_cdc_data()
    yield_df = read_write_yield_cdc_data()
    # Analysis 
    if isinstance(weather_df, pd.DataFrame) and len(weather_df)!=0:
        compute_and_store_analysis(weather_df)


if __name__ == '__main__':
    logging.basicConfig(filename='/app/logs/execution_logs.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')
    try:
        subprocess.run(["git", "clone", "--depth=1", url, os.getcwd()+'/app/Temp'])
    except Exception as e:
        logging.info(f"Problem when cloning -{e}")
    try:
        run_main()
    except Exception as e:
        logging.info(f'Exception orrured {e}')



