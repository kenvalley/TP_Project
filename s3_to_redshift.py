
import psycopg2
from aws_redshift_config import aws_configure
from web_scrap_app import scrapping_app
import configparser
from botocore.exceptions import ClientError

#scrapping_app = scrapping_app()

def execute():
    scrapping_app()

    # def conf():
    try:
        config = configparser.ConfigParser()
        config.read_file(open('encrypt.cfg'))

        # KEY                    = config.get('AWS','KEY')
        # SECRET                 = config.get('AWS','SECRET')
        # DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
        # DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
        # DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
        # DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
        # DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
        
        DWH_DB                 = config.get("DWH","DWH_DB")
        DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
        DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
        DWH_PORT               = config.get("DWH","DWH_PORT")



    except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                print("User already exists")
                pass
            else:
                print("Unexpected error: %s" % e)

    ENDPOINT = aws_configure()
                    
    conn = psycopg2.connect(dbname=DWH_DB, host=ENDPOINT, port=DWH_PORT,
                            user=DWH_DB_USER, password=DWH_DB_PASSWORD)
    cur = conn.cursor()

    print('---Connection Established---')


    # Begin transaction
    cur.execute("begin;")

    print('---Begin Transaction---')



    # Create table to place your data
    cur.execute("create table WEBSCRAP_TABLE (\
                INDEX VARCHAR(200),\
                ID VARCHAR(200), Link VARCHAR(500),	src VARCHAR(500), \
                Bank_name VARCHAR(500), Website VARCHAR(500), \
                Number_of_Reviews VARCHAR(500),	Reviews VARCHAR(500), \
                Rating_5_star VARCHAR(500), Rating_4_star VARCHAR(500),	Rating_3_star VARCHAR(500), \
                Rating_2_star VARCHAR(500),	Rating_1_star VARCHAR(500));")

    print('----Table Succesfully Created---')

    cur.execute("copy <TABLE_NAME> from 's3://BUCKET-NAME-project-01/ec2scrap_50.csv' credentials 'aws_access_key_id=...;aws_secret_access_key=...' delimiter ',' IGNOREHEADER as 1 csv;")

    print('---Succesfully Copied From S3')


    # Commit your transaction
    cur.execute("commit;")

    print("COPY executed fine!")


execute()