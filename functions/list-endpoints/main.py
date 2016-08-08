# Function to get a listing of what endpoints are allowed
#
# Perform a GET request against https://api.door43.org/tx to determine what endpoints and methods are allowed.

import boto3
import json

endpoints = {
    "version": "1",
    "links": [
        {
            "href": "/job",
            "rel": "list",
            "method": "GET"
        },
        {
            "href": "/job",
            "rel": "create",
            "method": "POST"
        },
    ]
}

client = boto3.client('lambda')

def handle(event, ctx):
    if 'user_token' not in event or not event['user_token']:
        return {"error": "user_token not given"}

    response = client.invoke(
        FunctionName='tx-manager_get-user-by-token',
        Payload=json.dumps({'user_token': event['user_token']}),
    )
    payload = json.loads(response['Payload'].read())
    if 'error' in payload:
        raise Exception('[BadRequest] {0}'.format(payload["error"]))
    elif 'user' not in payload:
        raise Exception('[BadRequest] user not found')

    return endpoints
