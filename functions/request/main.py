# Method for handling all tX-Manager requests

from __future__ import print_function

import boto3
import gogs_client
import hashlib
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Attr

def get_user(data):
    if 'user_token' not in data or not data['user_token']:
        raise Exception('[BadRequest] user_token not given')
    gogs_api = gogs_client.GogsApi("https://git.door43.org")
    success = gogs_api.valid_authentication(gogs_client.GogsToken(data['user_token']))
    if not success:
        raise Exception('[BadRequest] user_token invalid')
    return data['user_token']

def list_endpoints(data):
    user = get_user(data)
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

def register_module(data):
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
            raise Exception('[BadRequest] {0} not given'.format(field))

    for field in list_fields:
        if field in module:
            if not isinstance(module[field], list):
                module[field] = [module[field]]

    for field, value in enumerate(module):
        if field not in list_fields and isinstance(value, list):
            raise Exception('[BadRequest] {0} cannot be a list'.format(field))

    if 'public_links' not in module:
        module['public_links'] = []

    if 'private_links' not in module:
        module['private_links'] = []

    if 'version' not in module:
        module['version'] = 1

    table = dynamodb.Table(tablename)

    try:
        table.put_item(
            Item=module
        )
    except Exception as e:
        print("Error creating record for {0}:".format(module['name']))
        print(e)

    response = table.get_item(
        Key={
            'name': module['name']
        }
    )
    item = response['Item']
    return item

def start_job(data):
    user = get_user(data)
    tablename = 'tx-job'
    dynamodb = boto3.resource('dynamodb')

    if 'resource_type' not in data or not data['resource_type']:
        raise Exception("[BadRequest] resource_type not given")

    if 'input_format' not in data or not data['input_format']:
        raise Exception("[BadRequest] input_format not given")

    if 'output_format' not in data or not data['output_format']:
        raise Exception("[BadRequest] output_format not given")

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
        raise Exception("[BadRequest] no converter was found to convert {0} from {1} to {2}".format(data['resource_type'], data['input_format'], data['output_format']))

    cdn_url = 'https://cdn.door43.org'
    if 'cdn_url' in data:
        cdn_url = data['cdn_url']

    created_at = datetime.utcnow()
    eta = created_at + timedelta(minutes=2)
    expiration = eta + timedelta(days=1)
    job_id = hashlib.sha256('{0}-{1}-{2}'.format(data['user_token'], user, created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))).hexdigest()
    job = {
        'job_id': job_id,
        'user': user,
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

def list_jobs(data):
    user = get_user(data)
    tablename = 'tx-job'
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(tablename)
    response = table.scan(
        FilterExpression=Attr('user').eq(user)
    )
    return response['Items']

def handle(event, ctx):
    data = {}
    action = "endpoints"
    if 'data' in event:
        data = event['data']
    if 'action' in event:
        action = event['action']
    if 'body-json' in event and event['body-json'] and isinstance(event['body-json'], dict):
        data.update(event['body-json'])

    if action == 'endpoints':
        return list_endpoints(data)
    elif action == 'module':
        return register_module(data)
    elif action == 'job':
        if 'source' in data:
            return start_job(data)
        else:
            return list_jobs(data)
    else:
        raise Exception("[BadRequest] Invalid action '{0}'".format(action))


