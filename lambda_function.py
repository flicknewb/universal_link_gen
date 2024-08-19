import requests
import json
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from jsonschema import validate
from jsonschema.exceptions import ValidationError

schema = {
    "type": "object",
    "properties": {
        "firstName": {"type": "string"},
        "lastName": {"type": "string"},
        "email": {"type": "string"},
        "phone": {"type": "string"},
        "extRef": {"type": "string"},
        "embeddedData": {"type": "object"},
        "transactionData": {"type": "object"},
        "API_TOKEN": {"type": "string"},
        "DATACENTER": {"type": "string"},
        "DIRECTORY_ID": {"type": "string"},
        "MAILINGLIST_ID": {"type": "string"},
        "SURVEY_ID": {"type": "string"}
    },
    "required": ["firstName", "lastName", "email", "phone", "extRef", "embeddedData", "transactionData", "API_TOKEN", "DATACENTER", "DIRECTORY_ID", "MAILINGLIST_ID", "SURVEY_ID"]
}

## ******* FUNCTIONS ****** ##
# AWS LAMBDA SETUP TARGETS THE 'lambda_handler' FUNCTION IN THE 'lambda_function.py' FILE.
# This is the entry point for the API endpoint being called.


def lambda_handler(event, context):
    # CATCH VARIABLES FROM QUALTRICS WEB REQUEST
    # body = json.loads(event)
    print("Incoming Event:", event['body'], type(event['body']))
    body = json.loads(event['body'])
    # Qualtrics creds
    try:
        validate(instance=body, schema=schema)
    except ValidationError as e:
        message = {"error": e.message}
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(message),
        }
    try:
        api_token = body['API_TOKEN']
        datacenter = body['DATACENTER']
        directoryid = body['DIRECTORY_ID']
        mlid = body['MAILINGLIST_ID']
        survey_id = body['SURVEY_ID']
        # Create contact in mailing list
        url = "https://"+datacenter+".qualtrics.com/API/v3/directories/" + \
            directoryid+"/mailinglists/"+mlid+"/contacts"
        payload = {
            "firstName": body['firstName'],
            "lastName": body['lastName'],
            "email": body['email'],
            "phone": body['phone'],
            "extRef": body['extRef'],
            "embeddedData": body['embeddedData'],
            "unsubscribed": False
        }
        headers = {
            "Content-Type": "application/json",
            "X-API-TOKEN": api_token
        }
        response = requests.request("POST", url, json=payload, headers=headers)
        print(response.text)
        cid = json.loads(response.text)['result']['id']
        print("cid:", cid)

        # Create Transaction
        url2 = "https://"+datacenter + \
            ".qualtrics.com/API/v3/directories/"+directoryid+"/transactions"
        # dat_tim = "2016-12-05 15:45:04"
        dtnow = datetime.now().strftime('%Y-%m-%d %I:%M:%S')
        payload2 = {
            "usrTx": {
                "contactId": cid,
                "mailingListId": mlid,
                "transactionDate": dtnow,
                "data": body['transactionData']
            }
        }
        response2 = requests.request(
            "POST", url2, json=payload2, headers=headers)
        print(response2.text)
        tx_id = json.loads(response2.text)[
            'result']['createdTransactions']['usrTx']['id']
        print("transaction id:", tx_id)
        # Create Transaction Batch
        url3 = "https://"+datacenter+".qualtrics.com/API/v3/directories/" + \
            directoryid+"/transactionbatches"
        payload3 = {
            "transactionIds": [tx_id],
            "creationDate": datetime.now().strftime('%Y-%m-%dT%I:%M:%SZ')  # "2019-08-24T14:15:22Z"
        }
        response3 = requests.request(
            "POST", url3, json=payload3, headers=headers)
        print(response3.text)
        batch_id = json.loads(response3.text)['result']['id']
        print("batch Id:", batch_id)
        # Generate Distribution links
        url4 = "https://"+datacenter+".qualtrics.com/API/v3/distributions"
        payload4 = {
            "surveyId": survey_id,
            "linkType": "Individual",
            # "2019-06-24 00:00:00",
            "description": "distribution "+datetime.now().strftime('%Y-%m-%d %I:%M:%S'),
            "action": "CreateTransactionBatchdistribution",
            "transactionBatchId": batch_id,
            # "2019-07-24 00:00:00",
            "expirationDate": (datetime.now() + relativedelta(months=+2)).strftime('%Y-%m-%d %I:%M:%S'),
            "mailingListId": mlid
        }
        response4 = requests.request(
            "POST", url4, json=payload4, headers=headers)
        print(response4.text)
        dist_id = json.loads(response4.text)['result']['id']
        print("distribution ID:", dist_id)
        # Fetch Distribution links
        url5 = "https://"+datacenter+".qualtrics.com/API/v3/distributions/" + \
            dist_id+"/links?surveyId="+survey_id
        response5 = requests.request("GET", url5, headers=headers)
        print(response5.text)
        link = json.loads(response5.text)['result']['elements'][0]['link']
        print("dist link:", link)
    except Exception as e:
        message = {"error": e}
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(message),
        }
    message = {"link": link}
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(message),
    }
