from flask import Flask, request, jsonify
from flasgger import Swagger
import pandas as pd

import json
import sqlite3

app = Flask(__name__)
swagger = Swagger(app)
import mysql.connector


def getSqliteConnection():

    # """Connects to SQL database

    # Returns:
    #     cursor: Cursor object to access database
    #     conection: Database connection object
    # """

    cnx = mysql.connector.connect(
        host="mysql_db",
        user="root",
        password="password",
        port=3306
    )
    cursor = cnx.cursor()
    cursor.execute('USE corteva')
    return cursor, cnx


def get_pagination_link(page):
  #   """Returns next page endpoint

  #   Args:
  #       page (int): page number

  #   Returns:
  #       string: New page endpoint
  # """
    # generate a link to the specified page of results
    return f'/api/weather?page={page}'


@app.route('/api/weather', methods=["GET"])
def getWeather():
    """
        Weather endpoint which gives Maximum Temperature, Minimum Temperature, Precipitation each date and station IDs
        ---
        parameters:
          - name: page
            in: query
            type: number
            description: Pagination for the response
            default: 1
          - name: date
            in: query
            type: string
            description: Date for which you want to see weather.
            required: false
            default: ''
          - name: stationID
            in: query
            type: string
            description: Station ID for which you want to see weather
            required: false
            default: ''
        responses:
          200:
            description: A JSON object containing weather for requested to Date and StationID
            schema:
              type: object
              properties:
                Date:
                  type: string
                  description: 1985-01-01 00:00:00
                Maximum_temperature:
                  type: number
                  description: 30
                Minimum_temperature:
                  type: number
                  description: -4
                Precipitation:
                  type: number
                  description: 12
                Staton_ID:
                  type: string
                  description: USC00110072
        """
    page = int(request.args.get('page', 1))
    page_size = 50
    date = request.args.get('date', None)
    stationId = request.args.get('stationId',None )
    print(page, page_size, date, stationId)

    off = (page - 1) * page_size
    cursor, cnx = getSqliteConnection()

    query = f"""
                      SELECT Station_ID,Date,Maximum_Temperature,Minimum_Temperature,Precipitation
                      FROM weather limit 10 offset {off}"""
    if date and stationId:
        query = f"""
          SELECT Station_ID,Date,Maximum_Temperature,Minimum_Temperature,Precipitation
          FROM weather
          WHERE date ='{date}' AND Station_ID ='{stationId}' order by 1,2 limit 10 offset {off}"""

    elif date and not stationId:
        query = f"""
             SELECT Station_ID,Date,Maximum_Temperature,Minimum_Temperature,Precipitation
             FROM weather
             WHERE date ='{date}' order by 1,2 limit 10 offset {off}"""
    elif not date and stationId:
        query = f"""
                  SELECT Station_ID,Date,Maximum_Temperature,Minimum_Temperature,Precipitation
                  FROM weather
                  WHERE Station_ID ='{stationId}' order by 1,2 limit 10 offset {off}"""

    # Execute SQL query with parameters
    df = pd.read_sql(query, con=cnx)
    json_data = df.to_json(orient='records')

    response = {
        'results': json.loads(json_data),
        'pagination': {
            'next': get_pagination_link(page + 1)
        }
    }

    return jsonify(response)


@app.route('/api/weather/stats', methods=["GET"])
def getStats():
    """
        Weather endpoint which gives ....
        ---
        responses:
          200:
            description: A JSON object containing weather statistics for all station and years
            schema:
              type: object
              properties:
                Average_maximum_temperature:
                  type: number
                  description: 153.5480769231
                Average_minimum_temperature:
                  type: number
                  description: 15.6758241758
                Station_ID:
                  type: string
                  description: USC00110072
                Total_accumulated_precipitation:
                  type: number
                  description: 15544
                Year:
                  type: number
                  description: 1985

        """
    cursor, cnx = getSqliteConnection()
    # Write query and get results
    query = """
      SELECT Station_ID,Year,`Average maximum temperature`,`Average minimum temperature`,`Total accumulated precipitation` 
      FROM Weather_Stats"""

    # Execute SQL query with parameters
    df = pd.read_sql(query, con=cnx)
    print(df)
    json_data = df.to_json(orient='records')

    response = {
        'results': json.loads(json_data),
    }

    return jsonify(response)


if __name__ == '__main__':
    app.run()
