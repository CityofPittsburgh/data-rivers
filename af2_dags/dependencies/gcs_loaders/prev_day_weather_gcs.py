import os
import argparse
import time
from datetime import datetime
import requests
import json
import pytz
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
    date_time = datetime.fromtimestamp(hour['dt'])
    utc_conv = date_time.astimezone(tz=pytz.utc)
    est_conv = date_time.astimezone(tz=pytz.timezone('America/New_York'))
    unix_time = int(hour['dt'])

    utc_date_str = str(utc_conv)
    est_date_str = str(est_conv)

    temp = int(round(float(hour['temp'])))
    feel = int(round(float(hour['feels_like'])))
    humid = int(hour['humidity'])

    cond = str(hour['weather'][0]['main'])
    id = str(hour['weather'][0]['id'])
    icon = str(hour['weather'][0]['icon'])
    desc = str(hour['weather'][0]['description'])

    hour_dict = dict({"utc_date_time": utc_date_str, "est_date_time": est_date_str, "unix_date_time": unix_time,
                      "temp": temp, "feels_like": feel, "humidity": humid, "conditions": cond,
                      "icon_id": icon, "weather_id": id, "description": desc})
    datum.append(hour_dict)


avro_to_gcs(f"weather/{args['lookback_date'].split('-')[0]}/{args['lookback_date'].split('-')[1]}",
            f"{args['lookback_date']}_weather_report.avro",
            datum, bucket, "prev_day_weather.avsc")