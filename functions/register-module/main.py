# Function to register a module

from __future__ import print_function

import boto3

dynamodb = boto3.resource('dynamodb')
tablename = 'tx-module'

fields = ['name', 'version', 'public_links', 'private_links', 'type', 'input_format', 'output_format', 'resource_types', 'options']
required_fields = ['name', 'type', 'input_format', 'output_format', 'resource_types', 'options']
list_fields = ['public_links', 'private_links', 'input_format', 'output_format', 'resource_types', 'options']

def handle(event, ctx):
    module = {}
    for field in fields:
        if field in event:
            module[field] = event[field]

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
