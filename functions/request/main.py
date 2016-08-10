# Method for handling all tX-Manager requests

from __future__ import print_function

import boto3
import json
import requests

import gogs_client

from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key, Attr

class TXManager(object):
    
    def __init__(self, event, context):
        self.event = event
        self.context = context
        if 'data' in event:
            self.data = event['data']
        else:
            self.data = {}
        if 'body-json' in event and event['body-json'] and isinstance(event['body-json'], dict):
            self.data.update(event['body-json'])
        self.log = []
        self.errors = []
        self.warnings = []

    def log_message(self, message):
        print('{0}: {1}'.format('tx-manager', message))
        self.log.append('{0}: {1}'.format('tx-manager', message))

    def error_message(self, message):
        print('{0}: {1}'.format('tx-manager', message))
        self.errors.append('{0}: {1}'.format('tx-manager', message))

    def warning_message(self, message):
        print('{0}: {1}'.format('tx-manager', message))
        self.warnings.append('{0}: {1}'.format('tx-manager', message))

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

        print("module payload:")
        print(module)
        module_table = dynamodb.Table('tx-module')
        module_table.put_item(
            Item=module
        )
        return module

    def setup_job(self):
        dynamodb = boto3.resource('dynamodb')

        event = self.event
        context = self.context
        data = self.data
        
        user = self.get_user()

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

        if not self.get_converter(data):
            raise Exception('no converter was found to convert {0} from {1} to {2}'.format(data['resource_type'], data['input_format'], data['output_format']))
        
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
            "cdn_bucket": event["cdn_bucket"],
            "cdn_file": output_file,
            "output_expiration": expiration.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "links": {
                "href": "/job/{0}".format(job_id),
                "rel": "self",
                "method": "GET"
            }
        }
        
        if 'callback' in data and data['callback'] and data['callback'].startswith('http'):
            job['callback'] = data['callback']
        
        if 'options' in data and data['options']:
            job['options'] = data['options']

        # This is an identifier a client can send to be able to recognize this job, different than our unique job_id
        if 'identifier' in data:
            job['identifier'] = data['identifier']

        print('Job to save to tx-job:')
        print(job)

        # Saving this to the DynamoDB will start trigger a DB stream which will call
        # tx-manager again with the job info (see run() function)
        job_table = dynamodb.Table('tx-job')
        job_table.put_item(
            Item=job
        )

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

    def get_converter(self, job):
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('tx-module')
        response = table.scan()
        modules = response['Items']
        module = None
        for m in modules:
            if job['resource_type'] in m['resource_types']:
                if job['input_format'] in m['input_format']:
                    if job['output_format'] in m['output_format']:
                        module = m
        return module

    def start_job(self, job):
        self.log = []
        self.warnings = []
        self.errors = []

        dynamodb = boto3.resource('dynamodb')
        job_table = dynamodb.Table('tx-job')

        start_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        self.log_message("Started job {0} at {1}".format(job['job_id'], start_timestamp))

        try:
            job_table.update_item(
                Key={
                    'job_id': job['job_id'],
                },
                UpdateExpression="set start_timestamp = :start_timestamp",
                ExpressionAttributeValues={
                    ':start_timestamp': start_timestamp,
                }
            )
            print('Updated job in tx-job table with start_timestamp = {0}'.format(start_timestamp))

            module = self.get_converter(job)
            if not module:
                raise Exception('No converter was found to convert {0} from {1} to {2}'.format(job['resource_type'], job['input_format'], job['output_format']))

            job_table.update_item(
                Key={
                    'job_id': job['job_id'],
                },
                UpdateExpression="set convert_function = :convert_function",
                ExpressionAttributeValues={
                    ':convert_function': module['name']
                }
            )
            print('Updated job in tx-job table with convert_function = {0}'.format(module['name']))

            payload = {
                'data': {
                    'job': job,
                }
            }
            print("Payload to {0}:".format(module['name']))
            print(payload)

            self.log_message('Telling module {0} to convert {1}'.format(module['name'], job['output']))
            lambda_client = boto3.client('lambda')
            response = lambda_client.invoke(
                FunctionName=module['name'],
                Payload=json.dumps(payload)
            )
            response = json.loads(response['Payload'].read())
            print("Response payload from {0}:".format(module['name']))
            print(response)

            self.log.extend(response['log'])
            self.errors.extend(response['errors'])
            self.warnings.extend(response['warnings'])

            if response['errors']:
                self.log_message('{0} function returned with errors.'.format(module['name']))
            elif response['warnings']:
                self.log_message('{0} function returned with warnings.'.format(module['name']))
            if response['errors']:
                self.log_message('{0} function returned.'.format(module['name']))
        except Exception as e:
            self.error_message(e.message)

        if len(self.errors):
            success = False
            job_status = "fail"
            message = "Conversion failed"
        elif len(self.warnings) > 0:
            success = True
            job_status = "warning"
            message = "Conversion successful with warnings"
        else:
            success = True
            job_status = "success"
            message = "Conversion successful"

        self.log_message(message)

        end_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        self.log_message('Finished job {0} at {1}'.format(job['job_id'], end_timestamp))

        job_table.update_item(
            Key={
                'job_id': job['job_id'],
            },
            UpdateExpression="set end_timestamp = :end_timestamp, success = :success, job_status = :job_status, message = :message, logs = :logs, errors = :errors, warnings = :warnings",
            ExpressionAttributeValues={
                ':end_timestamp': end_timestamp,
                ':success': success,
                ':job_status': job_status,
                ':message': message,
                ':logs': json.dumps(self.log),
                ':errors': json.dumps(self.errors),
                ':warnings': json.dumps(self.warnings)
            }
        )
        print("Updated tx-job with end_timestamp, success, job_status, message, logs, errors and warnings.")
            
        response = {
            'job_id': job['job_id'],
            'identifier': job['identifier'],
            'success': success,
            'status': job_status,
            'message': message,
            'output': job['output'],
            'log': self.log,
            'warnings': self.warnings,
            'errors': self.errors,
            'start_timestamp': start_timestamp,
            'end_timestamp': end_timestamp
        }

        if 'callback' in job and job['callback'] and job['callback'].startswith('http'):
            callback_url = job['callback']
            headers = {"content-type": "application/json"}
            print('Making callback to {0} with payload:'.format(callback_url))
            print(response)
            response = requests.post(callback_url, json=response, headers=headers)
            print('finished.')

        return response

    def run(self):
        event = self.event
        
        if 'Records' in event:
            for record in event['Records']:
                if record['eventName'] == 'INSERT':
                    print(record['eventID'])
                    print(record['eventName'])
                    # print("DynamoDB Record: " + json.dumps(record['dynamodb'], indent=2))
                    job_id = record['dynamodb']['Keys']['job_id']['S']
                    job_table = boto3.resource('dynamodb').Table('tx-job')
                    response = job_table.get_item(
                        Key={
                            'job_id': job_id
                        }
                    )
                    job = response['Item']
                    if 'start_timestamp' not in job or not job['start_timestamp']:
                        self.start_job(job)
            return {
                'success': True,
                'message': 'Successfully processed {} records.'.format(len(event['Records']))
            }
        else:
            action = "endpoints"  # default action
            if 'action' in event:
                action = event['action']

            if action == 'endpoints':
                return self.list_endpoints()
            elif action == 'module':
                return self.register_module()
            elif action == 'job':
                if 'source' in self.data:
                    return self.setup_job()
                else:
                    return self.list_jobs()
            else:
                raise Exception('Invalid action')


def handle(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
     
    try:
        manager = TXManager(event, context)
        ret = manager.run()
    except Exception as e:
        print(e)
        print(e.message)
        e.message = 'Bad request: {0}'.format(e.message)
        raise e
    return ret
