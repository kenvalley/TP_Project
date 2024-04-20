drop database if exists weather_map_DB
create or replace database weather_map_DB

create warehouse if not exists weather_map_WH
create schema if not exists weather_map_SCHM

--CREATE STAGE (S3----SNOWFLAKE)
CREATE OR REPLACE STAGE weather_map_DB.weather_map_SCHM.snowflake_ext_stage url='s3://bucket-name-01/'
credentials=(
aws_key_id='AWS-KEY-HERE!'
aws_secret_key='AWS-SECRET-KEY-HERE!!!'
)

list @weather_map_DB.weather_map_SCHM.snowflake_ext_stage;

CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1;

select * from open_weather_snowflake

select city as city_sc_clouds
from open_weather_snowflake
where weather_description = 'scattered clouds'