from QualtricsAPI.Setup import Credentials
from QualtricsAPI.Survey import Responses
import pandas as pd
import requests
import json
import os

API_TOKEN = os.getenv("API_TOKEN")
DATACENTER = os.getenv("DATACENTER")


def lambda_handler(event, context):
    print("Incoming Event:", event['body'], type(event['body']))
    body = json.loads(event['body'])
    sids = ['SV_3xhKFtpxNaytUYm']
    Credentials().qualtrics_api_credentials(token=API_TOKEN,
                                            data_center=DATACENTER)
    r = Responses()
    for sid in sids:
        df = r.get_survey_responses(survey=sid)
        df.drop([0, 1], inplace=True)
        print(df.head())

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({}),
    }
