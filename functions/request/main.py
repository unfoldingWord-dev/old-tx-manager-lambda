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
    if 'gogs_url' not in event or not event['gogs_url']:
        raise Exception('"gogs_url" not in payload')
    if 'user_token' not in data or not data['user_token']:
        raise Exception('"user_token" not in payload')

    gogsUrl = event['gogs_url']
    gogsApi = gogs_client.GogsApi(gogsUrl)
    success = gogsApi.valid_authentication(gogs_client.GogsToken(data['user_token']))
    if not success:
        raise Exception('"user_token" invalid, needs to be a valid Gogs user at {0}'.format(event['gogs_url']))
    if 'username' in data and data['username']:
        return data['username']
    else:
        return data['user_token']


def list_endpoints():
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
    dynamodb = boto3.resource('dynamodb')
    lambdaClient = boto3.client('lambda')

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

    table = dynamodb.Table('tx-module')
    table.put_item(
        Item=module
    )

    response = table.get_item(
        Key={
            'name': module['name']
        }
    )
    item = response['Item']

    # Getting the ARN for the Function that is being registered so we can subscribe it to topics
    functionArn = None
    response = lambdaClient.get_function(FunctionName=module['name'])
    if response and 'Configuration' in response and 'FunctionArn' in response['Configuration']:
        functionArn = response['Configuration']['FunctionArn']
    print("function:")
    print(response)
    print(functionArn)

    messageStoreArn = None
    response = lambdaClient.get_function(FunctionName='SNS_messageStore')
    if response and 'Configuration' in response and 'FunctionArn' in response['Configuration']:
        messageStoreArn = response['Configuration']['FunctionArn']
    print("SNS_messageStore ARN:")
    print(response)
    print(messageStoreArn)

    # Creating a topic of each resource_type/input_format/output_format combination, and subscribe this module to it
    snsClient = boto3.client('sns')
    for rtype in module['resource_types']:
        for iformat in module['input_format']:
            for oformat in module['output_format']:
                topicName = '{0}-{1}2{2}'.format(rtype, iformat, oformat)
                response = snsClient.create_topic(Name=topicName)
                print("topicArn query response:")
                print(response)
                if response and 'TopicArn' in response:
                    topicArn = response['TopicArn']
                    print('TopicArn: {0}'.format(topicArn))
                    topicTable = dynamodb.Table('tx-topic')
                    topicTable.put_item(
                        Item={'TopicName': topicName, 'TopicArn': topicArn}
                    )

                    # Will register this function if there is an ARN for it
                    if functionArn:
                        response = snsClient.subscribe(
                            TopicArn=topicArn,
                            Protocol='lambda',
                            Endpoint=functionArn
                        )
                        print("subscribe:")
                        print(response)
                    # Adding tx-manager_messageLog so we can record the messages
                    snsClient.subscribe(
                        TopicArn=topicArn,
                        Protocol='lambda',
                        Endpoint=messageStoreArn
                    )
                    # Adding myself for testing purposes
                    snsClient.subscribe(
                        TopicArn=topicArn,
                        Protocol='email-json',
                        Endpoint='richmahnwa+sns@gmail.com'
                    )
                    # Adding my phone for teseting purposes
                    snsClient.subscribe(
                        TopicArn=topicArn,
                        Protocol='sms',
                        Endpoint='2084096665'
                    )
    return item


def broadcast_job():
    user = get_user()
    dynamodb = boto3.resource('dynamodb')

    if 'cdn_bucket' not in event:
        raise Exception('"cdn_bucket" not in payload')
    if 'source' not in data or not data['source']:
        raise Exception('"source" url not in payload')
    if 'resource_type' not in data or not data['resource_type']:
        raise Exception('"resource_type" not in payload')
    if 'input_format' not in data or not data['input_format']:
        raise Exception('"input_format" not in payload')
    if 'output_format' not in data or not data['output_format']:
        raise Exception('"output_format" not in payload')

    created_at = datetime.utcnow()
    eta = created_at + timedelta(minutes=2)
    expiration = eta + timedelta(days=1)
    job_id = context.aws_request_id # The JOB ID will simply be this Lambda instance's unique request ID
    outputFile = 'tx/job/{0}.zip'.format(job_id) # All conversions must result in a ZIP of the converted file(s)
    outputUrl = 'https://s3-us-west-2.amazonaws.com/{0}/{1}'.format(event["cdn_bucket"], outputFile)

    # Info to store to DynamoDB tx-job table
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

    print('Job to save to tx-job:')
    print(job)

    jobTable = dynamodb.Table('tx-job')
    jobTable.put_item(
        Item=job
    )

    # Will now broadcast this job to all subscribed to the <resource_type>-<input_format>2<output_format> topic
    snsClient = boto3.client('sns')
    # Going to get the ARN for the topic that we stored when creating it in register_module()
    topicTable = dynamodb.Table('tx-topic')
    topicName = '{0}-{1}2{2}'.format(job['resource_type'], job['input_format'], job['output_format'])
    response = topicTable.get_item(
        Key={
            'TopicName': topicName
        }
    )
    print("topic query response:")
    print(response)
    # If there is a Topic with this name, we will continue to try to broadcast...
    if response and 'Item' in response and 'TopicArn' in response['Item']:
        topicArn = response['Item']['TopicArn']
        response = snsClient.get_topic_attributes(TopicArn=topicArn)
        print('topic attributes request:')
        print(response)

        # Only broadcast if there are confirmed subscribers...
        if response and 'Attributes' in response and 'SubscriptionsConfirmed' in response['Attributes'] and response['Attributes']['SubscriptionsConfirmed'] > 0:
            subject = 'Job Request: {0} {1}'.format(topicName, job['job_id'])
            message = job['job_id']
            # Going to publish the job_id to the topic, so any converter(s) or other responders can handle this request
            response = snsClient.publish(TopicArn=topicArn, Message=message, Subject=subject)
            print('sns publish response:')
            print(response)

            if response and 'MessageId' in response:
                return {
                    "job": job,
                    "links": [
                        {
                            "href": "/job",
                            "rel": "list",
                            "method": "GET"
                        },
                        {
                            "href": "/job/{0}".format(job['job_id']),
                            "rel": "list",
                            "method": "GET"
                        },
                    ],
                }

    # There were no topic or no subscribers so we raise an error
    raise Exception("There are no tasks set up to convert {0} from {1} to {2}. Failed to convert.".format(job['resource_type'], job['input_format'], job['output_format']))


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
                ret = broadcast_job()
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


