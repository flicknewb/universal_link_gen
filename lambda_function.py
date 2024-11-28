from QualtricsAPI.Setup import Credentials
from QualtricsAPI.Survey import Responses
import pandas as pd
import requests
import json
import os

API_TOKEN = os.getenv("API_TOKEN")
DATACENTER = os.getenv("DATACENTER")
# from datetime import datetime
# from dateutil.relativedelta import relativedelta
# from jsonschema import validate
# from jsonschema.exceptions import ValidationError
# # Updated schema
# schema = {
#     "type": "object",
#     "properties": {
#         "firstName": {"type": "string"},
#         "lastName": {"type": "string"},
#         "email": {"type": "string"},
#         "phone": {"type": "string"},
#         "extRef": {"type": "string"},
#         "embeddedData": {"type": "object"},
#         "transactionData": {"type": "object"},
#         "DIRECTORY_ID": {"type": "string"},
#         "MAILINGLIST_ID": {"type": "string"},
#         "SURVEY_ID": {"type": "string"}
#     },
#     "required": ["DIRECTORY_ID", "MAILINGLIST_ID", "SURVEY_ID"]
# }
# AWS Lambda entry point


def lambda_handler(event, context):
    print("Incoming Event:", event['body'], type(event['body']))
    body = json.loads(event['body'])
    sids = ['']
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
