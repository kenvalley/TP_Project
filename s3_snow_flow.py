
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from open_weather_info import weather_api_info

default_args = {
    'owner':'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024,4,16),
    'email':[],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

bucket = "bucket-name-01"
s3_prefix = f"s3://{bucket}/current_weather_info.csv"
s3_bucket = None

with DAG('open_weather_s3_to_snowflake', 
         default_args = default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    extract_open_weather_rapid_api_data = PythonOperator(
        task_id = 'extract_open_weather_data',
        python_callable = weather_api_info,
        dag=dag
    )


    is_file_now_available_in_s3 = S3KeySensor(
        task_id = 'tsk_is_file_now_available_in_s3',
        bucket_key= s3_prefix,
        bucket_name= s3_bucket,
        aws_conn_id= 'aws_s3_conn',
        wildcard_match= False,
        timeout = 60,
        poke_interval = 10
    )

    create_table = SnowflakeOperator(
        task_id = 'create_snowflake_table',
        snowflake_conn_id='conn_id_snowflake',
        sql=''' DROP TABLE IF EXISTS open_weather_snowflake;
                CREATE TABLE IF NOT EXISTS open_weather_snowflake(
                    city TEXT NOT NULL,	
                    weather_description varchar(100) NOT NULL,	
                    temp_fahr NUMERIC NOT NULL,	
                    feels_like NUMERIC NOT NULL,	
                    min_temp_fahr NUMERIC NOT NULL, 
                    max_temp_fahr NUMERIC NOT NULL,	
                    Pressure NUMERIC NOT NULL,	
                    humidity NUMERIC NOT NULL,	
                    wind_speed NUMERIC NOT NULL,	
                    date_of_capture varchar(100)
                )
        '''
    )

    copy_csv_into_snowflake_table_from_s3 = SnowflakeOperator(
        task_id = 'task_copy_csv_into_snowflake_table_from_s3',
        snowflake_conn_id='conn_id_snowflake',
        sql=''' COPY INTO weather_map_DB.weather_map_SCHM.open_weather_snowflake FROM @weather_map_DB.weather_map_SCHM.snowflake_ext_stage FILE_FORMAT = csv_format'''
    )

    extract_open_weather_rapid_api_data >> is_file_now_available_in_s3 >> create_table >> copy_csv_into_snowflake_table_from_s3
