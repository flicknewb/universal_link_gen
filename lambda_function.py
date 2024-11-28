from QualtricsAPI.Setup import Credentials
from QualtricsAPI.Survey import Responses
import sqlalchemy
import pandas as pd
import requests
import json
import os

API_TOKEN = os.getenv("API_TOKEN")
DATACENTER = os.getenv("DATACENTER")


def lambda_handler(event, context):
    print("Incoming Event:", event['body'], type(event['body']))
    body = json.loads(event['body'])
    # Initilize Qualtrics Variables:
    sids = ['SV_3xhKFtpxNaytUYm']
    Credentials().qualtrics_api_credentials(token=API_TOKEN,
                                            data_center=DATACENTER)
    r = Responses()
    # Initilize DB connection
    # DB ENDPOINT
    endpoint = os.environ['DB_DOMAIN']
    username = os.environ['DB_USERNAME']
    password = os.environ['DB_PASS']
    database_name = os.environ['DB_NAME']
    table_name = os.environ['TABLE_NAME']
    # SQL INIT
    engine = sqlalchemy.create_engine(
        f'mysql+pymysql://{username}:{password}@{endpoint}/{database_name}').connect()
    survey_data_table = pd.read_sql_table(table_name, engine)

    # Iterate the surveys
    for sid in sids:
        df = r.get_survey_responses(survey=sid)
        df.drop([0, 1], inplace=True)
        print(df.head())
        # Load the data into our table

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({}),
    }
