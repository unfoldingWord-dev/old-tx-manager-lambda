# Method for handling all tX-Manager requests

from __future__ import print_function

import boto3
import json
import gogs_client

from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Attr

data = {}
event = {}
context = {}

def get_user():
    if 'user_token' not in data or not data['user_token']:
        raise Exception('user_token not given')
    gogs_url = "https://git.door43.org"
    if 'gogs_url' in event:
        gogs_url = event['gogs_url']
    gogs_api = gogs_client.GogsApi(gogs_url)
    success = gogs_api.valid_authentication(gogs_client.GogsToken(data['user_token']))
    if not success:
        raise Exception('user_token invalid')
    if 'username' in data and data['username']:
        return data['username']
    else:
        return data['user_token']

def list_endpoints():
    user = get_user()
    return {
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

def register_module():
    tablename = 'tx-module'
    dynamodb = boto3.resource('dynamodb')

    fields = ['name', 'version', 'public_links', 'private_links', 'type', 'input_format', 'output_format', 'resource_types', 'options']
    required_fields = ['name', 'type', 'input_format', 'output_format', 'resource_types']
    list_fields = ['public_links', 'private_links', 'input_format', 'output_format', 'resource_types', 'options']

    module = {}
    for field in fields:
        if field in data:
            module[field] = data[field]

    for field in required_fields:
        if field not in module:
            raise Exception('{0} not given'.format(field))

    for field in list_fields:
        if field in module:
            if not isinstance(module[field], list):
                module[field] = [module[field]]

    for field, value in enumerate(module):
        if field not in list_fields and isinstance(value, list):
            raise Exception('{0} cannot be a list'.format(field))

    if 'public_links' not in module:
        module['public_links'] = []

    if 'private_links' not in module:
        module['private_links'] = []

    if 'version' not in module:
        module['version'] = 1

    table = dynamodb.Table(tablename)

    table.put_item(
        Item=module
    )

    response = table.get_item(
        Key={
            'name': module['name']
        }
    )
    item = response['Item']
    return item

def start_job():
    user = get_user()
    tablename = 'tx-job'
    dynamodb = boto3.resource('dynamodb')

    if 'source' not in data or not data['source']:
        raise Exception('source url not given')
    if 'resource_type' not in data or not data['resource_type']:
        raise Exception('resource_type not given')
    if 'input_format' not in data or not data['input_format']:
        raise Exception('input_format not given')
    if 'output_format' not in data or not data['output_format']:
        raise Exception('output_format not given')

    table = dynamodb.Table('tx-module')
    response = table.scan()
    modules = response['Items']
    module = None
    for m in modules:
        if data['resource_type'] in m['resource_types']:
            if data['input_format'] in m['input_format']:
                if data['output_format'] in m['output_format']:
                    module = m

    if not module:
        raise Exception('no converter was found to convert {0} from {1} to {2}'.format(data['resource_type'], data['input_format'], data['output_format']))

    cdn_bucket = 'cdn.door43.org'
    if 'cdn_bucket' in event:
        cdn_bucket = event['cdn_bucket']

    created_at = datetime.utcnow()
    eta = created_at + timedelta(minutes=2)
    expiration = eta + timedelta(days=1)
    job_id = context.aws_request_id
    outputFile = 'tx/job/{0}.zip'.format(job_id)
    outputUrl = 'https://{0}/{1}'.format(cdn_bucket, outputFile)
    job = {
        'job_id': job_id,
        'user': user,
        'source': data['source'],
        'resource_type': data['resource_type'],
        'input_format': data['input_format'],
        'output_format': data['output_format'],
        'created_at': created_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        'eta': eta.strftime("%Y-%m-%dT%H:%M:%SZ"),
        'output': outputUrl,
        "output_expiration": expiration.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "links": {
            "href": "/job/{0}".format(job_id),
            "rel": "self",
            "method": "GET"
        }
    }

    if 'callback' in data and data['callback']:
        job['callback'] = data['callback']

    if 'options' in data and data['options']:
        job['options'] = data['options']

    print(job)
    table = dynamodb.Table(tablename)
    table.put_item(
        Item=job
    )

    payload = {
        'data': {
            'job': job,
            's3_bucket': cdn_bucket,
            's3_file': outputFile
        }
    }

    print("Payload to {0}:".format(module['name']))
    print(payload)

    lambda_client = boto3.client('lambda')
    response = lambda_client.invoke(
        FunctionName=module['name'],
        Payload=json.dumps(payload)
    )
    responsePayload = json.loads(response['Payload'].read())

    print("Response payload from {0}:".format(module['name']))
    print(responsePayload)

    if 'errorMessage' in responsePayload:
        raise Exception('{0}'.format(responsePayload["errorMessage"]))

    return {
        'job': job,
        "links": [
            {
                "href": "/job",
                "rel": "list",
                "method": "GET"
            },
        ],
        'response': responsePayload
    }

def list_jobs():
    user = get_user()
    tablename = 'tx-job'
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(tablename)
    response = table.scan(
        FilterExpression=Attr('user').eq(user)
    )
    return response['Items']

def handle(e, ctx):
    global event, context, data
    event = e
    context = ctx

    action = "endpoints"
    if 'data' in event:
        data = event['data']
    if 'action' in event:
        action = event['action']
    if 'body-json' in event and event['body-json'] and isinstance(event['body-json'], dict):
        data.update(event['body-json'])

#    try:
    if True:
        if action == 'endpoints':
            ret = list_endpoints()
        elif action == 'module':
            ret = register_module()
        elif action == 'job':
            if 'source' in data:
                ret = start_job()
            else:
                ret = list_jobs()
        else:
            raise Exception('Invalid action')
    # except Exception as e:
    #     print(e)
    #     print(e.message)
    #     e.message = 'Bad request: {0}'.format(e.message)
    #     raise e
    return ret
