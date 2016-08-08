# Listing of jobs by user

from __future__ import print_function

import boto3
import json
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.resource('dynamodb')
client = boto3.client('lambda')
tablename = 'tx-job'

def handle(event, ctx):
    if 'user_token' not in event or not event['user_token']:
        raise Exception("[BadRequest] user_token not given")

    response = client.invoke(
        FunctionName='tx-manager_get-user-by-token',
        Payload=json.dumps({'user_token': event['user_token']}),
    )
    payload = json.loads(response['Payload'].read())
    if 'error' in payload:
        raise Exception('[BadRequest] {0}'.format(payload["error"]))
    elif 'user' not in payload:
        raise Exception('[BadRequest] user not found')
    user = payload['user']

    table = dynamodb.Table(tablename)
    response = table.scan(
        FilterExpression=Attr('user').eq(user)
    )
    return response['Items']
