import requests
import json
import pandas as pd
import configparser
from datetime import datetime


city_names = ['Glasgow', 'coventry', 'London', 'Abuja', 'Texas', 'Atlanta', 'Lagos', 'Paris', 'Dublin', 'Manchester', 'Edinburgh', 'Liverpool']
base_url = 'https://api.openweathermap.org/data/2.5/weather?q='

with open('/home/ubuntu/airflow/dags/config_folder/credentials.txt', 'r') as f:
    api_key = f.read()

noww = datetime.now()
data_string = noww.strftime("%d_%m_%Y_%H_%M_%S")
data_string_1 = f"current_weather_info_{data_string}"


def weather_api_info():
        
    df_one = pd.DataFrame()
    for city_name in city_names:
        
        full_url = f"{base_url}{city_name}&appid={api_key}"    
        req = requests.get(full_url)
        data = req.json()

        def kelvin_to_fahr(temp_in_kelvin):
            temp_in_fahr = ((temp_in_kelvin - 273.15)* (9/5) + 32)
            return temp_in_fahr

        city = data['name']
        weather_description = data['weather'][0]['description']
        temp_fahr = data['main']['temp']
        feels_like = kelvin_to_fahr(data['main']['feels_like'])
        min_temp_fahr = kelvin_to_fahr(data['main']['temp_min'])
        max_temp_fahr = kelvin_to_fahr(data['main']['temp_max'])
        Pressure = data['main']['pressure']
        humidity = data['main']['humidity']
        wind_speed = data['wind']['speed']

        transformed_data = {
            'city': city,
            'weather_description': weather_description,
            'temp_fahr':temp_fahr,
            'feels_like': round(feels_like, 2),
            'min_temp_fahr': round(min_temp_fahr, 2),
            'max_temp_fahr': round(max_temp_fahr, 2),
            'Pressure': Pressure,
            'humidity': humidity,
            'wind_speed': wind_speed,
            'date_of_capture': datetime.now()
        }
        transformed_data_list = [transformed_data]
        df_1 = pd.DataFrame(transformed_data_list)
        df_one = pd.concat([df_one, df_1], ignore_index=True)

    # df_one.to_csv(f"{data_string_1}.csv", index=False)
    # print(df_one)

    config = configparser.ConfigParser()
    config.read_file(open('/home/ubuntu/airflow/dags/config_folder/encrypt.cfg'))
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    bucket = "bucket-name-01"
    return df_one.to_csv(f"s3://{bucket}/current_weather_info.csv", storage_options={'key': KEY, 'secret': SECRET}, index=False)
