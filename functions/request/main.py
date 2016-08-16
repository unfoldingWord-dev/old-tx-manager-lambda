# Method for handling all tX-Manager requests

from __future__ import print_function

import boto3
import json
import gogs_client
import hashlib
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
    required_fields = ['name', 'type', 'input_format', 'output_format', 'resource_types', 'options']
    list_fields = ['public_links', 'private_links', 'input_format', 'output_format', 'resource_types', 'options']

    module = {}
    for field in fields:
        if field in data:
            module[field] = data[field]

    for field in required_fields:
        if field not in module or not module[field]:
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

    cdn_url = 'https://cdn.door43.org'
    if 'cdn_url' in event:
        cdn_url = event['cdn_url']

    created_at = datetime.utcnow()
    eta = created_at + timedelta(minutes=2)
    expiration = eta + timedelta(days=1)
    #job_id = hashlib.sha256('{0}-{1}-{2}'.format(data['user_token'], user, created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))).hexdigest()
    job_id = context.aws_request_id
    job = {
        'job_id': job_id,
        'user': user,
        'source': data['source'],
        'resource_type': data['resource_type'],
        'input_format': data['input_format'],
        'output_format': data['output_format'],
        'created_at': created_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        'eta': eta.strftime("%Y-%m-%dT%H:%M:%SZ"),
        'output': "{0}/tx/job/{1}.{2}".format(cdn_url, job_id, data['output_format']),
        "output_expiration": expiration.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "links": {
            "href": "/job/{0}".format(job_id),
            "rel": "self",
            "method": "GET"
        }
    }

    if 'callback' in data:
        job['callback'] = data['callback']
    else:
        job['callback'] = ""

    if 'options' in data:
        job['options'] = data['options']
    else:
        job['options'] = []

    table = dynamodb.Table(tablename)
    table.put_item(
        Item=job
    )

    lambda_client = boto3.client('lambda')
    response = lambda_client.invoke(
        FunctionName=module['name'],
        Payload=json.dumps(job)
    )
    payload = json.loads(response['Payload'].read())
    if 'error' in payload:
        raise Exception('{0}'.format(payload["error"]))

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

    ret = ""
    try:
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
    except Exception as e:
        ret = {'status':400,'error':'Bad Request: {0}'.format(str(e))}
    return ret
