DROP DATABASE IF EXISTS weather_map_DB
CREATE OR REPLACE DATABASE weather_map_DB
CREATE WAREHOUSE IF NOT EXISTS weather_map_WH
CREATE SCHEMA IF NOT EXISTS weather_map_SCHM

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

SELECT * FROM open_weather_snowflake

SELECT city AS city_sc_clouds
FROM open_weather_snowflake
WHERE weather_description = 'scattered clouds'