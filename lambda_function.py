import requests
import json
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from jsonschema import validate
from jsonschema.exceptions import ValidationError
# Updated schema
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
        "DIRECTORY_ID": {"type": "string"},
        "MAILINGLIST_ID": {"type": "string"},
        "SURVEY_ID": {"type": "string"}
    },
    "required": ["DIRECTORY_ID", "MAILINGLIST_ID", "SURVEY_ID"]
}
# AWS Lambda entry point


def lambda_handler(event, context):
    print("Incoming Event:", event['body'], type(event['body']))
    body = json.loads(event['body'])
    # Validate the incoming JSON body
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
        directoryid = body['DIRECTORY_ID']
        mlid = body['MAILINGLIST_ID']
        survey_id = body['SURVEY_ID']
        api_token = os.getenv("API_TOKEN")
        datacenter = os.getenv("DATACENTER")
        # Create contact in mailing list
        url = "https://"+datacenter+".qualtrics.com/API/v3/directories/" + \
            directoryid+"/mailinglists/"+mlid+"/contacts"
        # Build the payload with conditional inclusion
        payload = {}
        if "firstName" in body:
            payload["firstName"] = body['firstName']
        if "lastName" in body:
            payload["lastName"] = body['lastName']
        if "email" in body:
            payload["email"] = body['email']
        if "phone" in body:
            payload["phone"] = body['phone']
        if "extRef" in body:
            payload["extRef"] = body['extRef']
        if "embeddedData" in body:
            payload["embeddedData"] = body['embeddedData']
        payload["unsubscribed"] = False
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
            ".qualtrics.com/API/v3/directories/" + directoryid + "/transactions"
        dtnow = datetime.now().strftime('%Y-%m-%d %I:%M:%S')
        payload2 = {
            "usrTx": {
                "contactId": cid,
                "mailingListId": mlid,
                "transactionDate": dtnow,
                # Safely get transactionData or an empty dict
                "data": body.get('transactionData', {})
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
            "description": "distribution "+datetime.now().strftime('%Y-%m-%d %I:%M:%S'),
            "action": "CreateTransactionBatchdistribution",
            "transactionBatchId": batch_id,
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
        message = {"error": str(e)}  # Ensure the error is properly serialized
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
