# Method for handling all tX-Manager requests

from __future__ import print_function

import boto3
import json
import gogs_client

from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Attr


class TXManager(object):
    
    def __init__(self, event, context):
        self.event = event
        self.context = context
        if 'data' in event:
            self.data = event['data']
        else:
            raise Exception('"data" not found in the event.')

    def get_user(self):
        event = self.event
        data = self.data

        if 'gogs_url' not in event or not event['gogs_url']:
            raise Exception('"gogs_url" not in payload')
        if 'user_token' not in data or not data['user_token']:
            raise Exception('"user_token" not in payload')
        
        gogs_url = event['gogs_url']
        gogs_api = gogs_client.GogsApi(gogs_url)
        success = gogs_api.valid_authentication(gogs_client.GogsToken(data['user_token']))
        if not success:
            raise Exception('"user_token" invalid, needs to be a valid Gogs user at {0}'.format(event['gogs_url']))
        if 'username' in data and data['username']:
            return data['username']
        else:
            return data['user_token']

    def list_endpoints(self):
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

    def register_module(self):
        data = self.data

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
        
        txmodule_table = dynamodb.Table('tx-module')
        txmodule_table.put_item(
            Item=module
        )
        
        response = txmodule_table.get_item(
            Key={
                'name': module['name']
            }
        )
        item = response['Item']
        
        # Getting the ARM for the messageLog function so we can store all broadcasted messages,
        # used below in subscribing to topics
        message_store_arn = None
        response = lambdaClient.get_function(FunctionName='SNS_messageStore')
        if response and 'Configuration' in response and 'FunctionArn' in response['Configuration']:
            message_store_arn = response['Configuration']['FunctionArn']
        print("SNS_messageStore ARN:")
        print(response)
        print(message_store_arn)
        
        # Creating a topic of each resource_type/input_format/output_format combination, and subscribe this module to it
        events = ['request', 'started', 'completed', 'failed']
        sns_client = boto3.client('sns')
        for rtype in module['resource_types']:
            for iformat in module['input_format']:
                for oformat in module['output_format']:
                    for event in events:
                        topic_name = '{0}-{1}2{2}-{3}'.format(rtype, iformat, oformat, event)
                        response = sns_client.create_topic(Name=topic_name)
                        print("topic_arn query response:")
                        print(response)
                        if response and 'TopicArn' in response:
                            topic_arn = response['TopicArn']
                            print('TopicArn: {0}'.format(topic_arn))
                            topicTable = dynamodb.Table('tx-topic')
                            topicTable.put_item(
                                Item={'TopicName': topic_name, 'TopicArn': topic_arn}
                            )
        
                            # Will register the module's function for the 'request' event if there is an ARN for it
                            if event == 'request':
                                # Getting the ARN for the function of the module that is being registered so we
                                # can subscribe it to topics
                                response = lambdaClient.get_function(FunctionName=module['name'])
                                print("get_function({0}) response:".format(module['name']))
                                print(response)
                                if response and 'Configuration' in response and 'FunctionArn' in response['Configuration']:
                                    funtion_arn = response['Configuration']['FunctionArn']
                                    print(funtion_arn)
                                    response = sns_client.subscribe(
                                        TopicArn=topic_arn,
                                        Protocol='lambda',
                                        Endpoint=funtion_arn
                                    )
                                    print("subscribe() response:")
                                    print(response)
                            # All other events we want tx-manager to handle
                            else:
                                # Getting the ARN for the tx-manager_<event> functions to handle job events
                                response = lambdaClient.get_function(FunctionName='tx-manager_{0}'.format(event))
                                print('get_function(tx-manager_{0}) response:'.format(event))
                                if response and 'Configuration' in response and 'FunctionArn' in response['Configuration']:
                                    funtion_arn = response['Configuration']['FunctionArn']
                                    print(funtion_arn)
                                    if funtion_arn:
                                        response = sns_client.subscribe(
                                            TopicArn=topic_arn,
                                            Protocol='lambda',
                                            Endpoint=funtion_arn
                                        )
                                        print("subscribe() response:")
                                        print(response)
                                        response = sns_client.subscribe(
                                            TopicArn=topic_arn,
                                            Protocol='lambda',
                                            Endpoint=funtion_arn
                                        )
                                        print("subscribe:")
                                        print(response)
        
                            # Adding tx-manager_messageLog so we can record the messages
                            sns_client.subscribe(
                                TopicArn=topic_arn,
                                Protocol='lambda',
                                Endpoint=message_store_arn
                            )
                            # Adding myself for testing purposes
                            sns_client.subscribe(
                                TopicArn=topic_arn,
                                Protocol='email-json',
                                Endpoint='richmahnwa+sns@gmail.com'
                            )
                            # Adding my phone for testing purposes
                            sns_client.subscribe(
                                TopicArn=topic_arn,
                                Protocol='sms',
                                Endpoint='208-409-6665'
                            )
        return item

    def broadcast_job(self):
        event = self.event
        context = self.context
        data = self.data
        
        user = self.get_user()
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
        output_file = 'tx/job/{0}.zip'.format(job_id) # All conversions must result in a ZIP of the converted file(s)
        output_url = 'https://s3-us-west-2.amazonaws.com/{0}/{1}'.format(event["cdn_bucket"], output_file)
        
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
            'output': output_url,
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
        
        job_table = dynamodb.Table('tx-job')
        job_table.put_item(
            Item=job
        )

        # OLD WAY TO CALL JOB - REMOVE
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
        payload = {
            'data': {
                'job': job,
                'cdn_bucket': event['cdn_bucket'],
                'cdn_file': output_file
             }
        }
        print("Payload to {0}:".format(module['name']))
        print(payload)
        lambdaClient = boto3.client('lambda')
        response = lambdaClient.invoke(
            FunctionName=module['name'],
            Payload=json.dumps(payload)
        )
        responsePayload = json.loads(response['Payload'].read())
        print("Response payload from {0}:".format(module['name']))
        print(responsePayload)
        if 'errorMessage' in responsePayload:
            raise Exception('{0}'.format(responsePayload["errorMessage"]))



        # Will now broadcast this job to all subscribed to the <resource_type>-<input_format>2<output_format> topic
        sns_client = boto3.client('sns')
        # Going to get the ARN for the topic that we stored when creating it in register_module()
        topicTable = dynamodb.Table('tx-topic')
        topic_name = '{0}-{1}2{2}-request'.format(job['resource_type'], job['input_format'], job['output_format'])
        response = topicTable.get_item(
            Key={
                'TopicName': topic_name
            }
        )
        print("topic query response:")
        print(response)
        # If there is a Topic with this name, we will continue to try to broadcast...
        if response and 'Item' in response and 'TopicArn' in response['Item']:
            topic_arn = response['Item']['TopicArn']
            response = sns_client.get_topic_attributes(TopicArn=topic_arn)
            print('topic attributes request:')
            print(response)
        
            # Only broadcast if there are confirmed subscribers...
            if response and 'Attributes' in response and 'SubscriptionsConfirmed' in response['Attributes'] and response['Attributes']['SubscriptionsConfirmed'] > 0:
                subject = 'Job Request: {0} {1}'.format(topic_name, job['job_id'])
                message = job['job_id']
                # Going to publish the job_id to the topic, so any converter(s) or
                # other responders can handle this request
                response = sns_client.publish(TopicArn=topic_arn, Message=message, Subject=subject)
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
#New way:
#        raise Exception("There are no tasks set up to convert {0} from {1} to {2}. Failed to convert.".format(job['resource_type'], job['input_format'], job['output_format']))
#Old way:
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


    def list_jobs(self):
        user = self.get_user()
        dynamodb = boto3.resource('dynamodb')
        job_table = dynamodb.Table('tx-job')
        response = job_table.scan(
            FilterExpression=Attr('user').eq(user)
        )
        return response['Items']


def handle(event, context):
    action = "endpoints" # default action
    if 'data' in event:
        data = event['data']
    if 'action' in event:
        action = event['action']
    if 'body-json' in event and event['body-json'] and isinstance(event['body-json'], dict):
        if data:
            data.update(event['body-json'])
        else:
            data = event['body-json']

    manager = TXManager(event, context)

    #    try:
    if True:
        if action == 'endpoints':
            ret = manager.list_endpoints()
        elif action == 'module':
            ret = manager.register_module()
        elif action == 'job':
            if 'source' in data:
                ret = manager.broadcast_job()
            else:
                ret = manager.list_jobs()
        else:
            raise Exception('Invalid action')
    # except Exception as e:
    #     print(e)
    #     print(e.message)
    #     e.message = 'Bad request: {0}'.format(e.message)
    #     raise e
    return ret


