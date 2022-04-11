import os
import argparse
import time
from datetime import datetime
import requests
import json
import tzlocal

from gcs_utils import json_to_gcs, avro_to_gcs

parser = argparse.ArgumentParser()
parser.add_argument('-l', '--lookback_date', dest='lookback_date',
                    required=True, help='Previous DAG execution date (YYYY-MM-DD)')
args = vars(parser.parse_args())

BASE_URL = 'http://api.openweathermap.org/data/2.5/onecall/timemachine'
PGH_LAT = '40.440624'
PGH_LONG = '-79.995888'
UNITS = 'imperial'

bucket = f"{os.environ['GCS_PREFIX']}_weather"

lookback_date = datetime.strptime(args['lookback_date'], '%Y-%m-%d')
lookback_api_date = datetime(lookback_date.year, lookback_date.month, lookback_date.day, 1, 0, 0)
timestamp = int(lookback_api_date.timestamp())

payload = {'lat': PGH_LAT,
           'lon': PGH_LONG,
           'appid': os.environ['OPENWEATHER_APPID'],
           'units': UNITS,
           'dt': timestamp
}

# API call to get data
response = requests.get(BASE_URL, params=payload)

# convert the API response text to json
tempWeatherDict = json.loads(response.text)

# retrieve hourly data and load to gcs
hourly_conditions = tempWeatherDict['hourly']

datum = []

for hour in hourly_conditions:
    date_stamp = (datetime.fromtimestamp(hour['dt'])).date()
    hour_stamp = (datetime.fromtimestamp(hour['dt'])).time()
    date_str = str(date_stamp) + " " + str(hour_stamp)


    temp = int(round(float(hour['temp'])))
    feel = int(round(float(hour['feels_like'])))
    humid = int(hour['humidity'])

    cond = str(hour['weather'][0]['main'])
    id = str(hour['weather'][0]['id'])
    desc = str(hour['weather'][0]['description'])

    hour_dict = dict({"date_time": date_str, "temp": temp, "feels_like": feel,
                      "humidity": humid, "conditions": cond, "icon_id": id,
                      "description": desc})
    datum.append(hour_dict)


avro_to_gcs(f"weather/{args['lookback_date'].split('-')[0]}/{args['lookback_date'].split('-')[1]}",
            f"{args['lookback_date']}_weather_report.avro",
            datum, bucket, "prev_day_weather.avsc")