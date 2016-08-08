from __future__ import print_function

import boto3
import json
import hashlib
from datetime import datetime, timedelta

dynamodb = boto3.resource('dynamodb')
client = boto3.client('lambda')
tablename = 'tx-job'

def handle(event, ctx):
    print("EVENT:")
    print(event)

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

    if 'resource_type' not in event or not event['resource_type']:
        raise Exception("[BadRequest] resource_type not given")

    if 'input_format' not in event or not event['input_format']:
        raise Exception("[BadRequest] input_format not given")

    if 'output_format' not in event or not event['output_format']:
        raise Exception("[BadRequest] output_format not given")

    table = dynamodb.Table('tx-module')
    response = table.scan()
    modules = response['Items']
    module = None
    for m in modules:
        if event['resource_type'] in m['resource_types']:
            if event['input_format'] in m['input_format']:
                if event['output_format'] in m['output_format']:
                    module = m

    if not module:
        raise Exception("[BadRequest] no converter was found to convert {0} from {1} to {2}".format(event['resource_type'], event['input_format'], event['output_format']))

    created_at = datetime.utcnow()
    eta = created_at + timedelta(minutes=2)
    expiration = eta + timedelta(days=1)
    job_id = hashlib.sha256('{0}-{1}-{2}'.format(event['user_token'], user, created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))).hexdigest()
    job = {
        'job_id': job_id,
        'user': user,
        'resource_type': event['resource_type'],
        'input_format': event['input_format'],
        'output_format': event['output_format'],
        'created_at': created_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        'eta': eta.strftime("%Y-%m-%dT%H:%M:%SZ"),
        'output': "https://test-cdn.door43.org/tx/jobs/{0}.{3}".format(job_id, event['output_format']),
        "output_expiration": expiration.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "links": {
            "href": "/jobs/{0}".format(job_id),
            "rel": "self",
            "method": "GET"
        }
    }

    if 'callback' in event:
        job['callback'] = event['callback']
    else:
        job['callback'] = ""

    if 'options' in event:
        job['options'] = event['options']
    else:
        job['options'] = []

    print("JOB:")
    print(job)

    table = dynamodb.Table(tablename)
    table.put_item(
        Item=job
    )

    return {
        'job': job,
        "links": [
            {
                "href": "/job",
                "rel": "list",
                "method": "GET"
            },
        ]
    }
