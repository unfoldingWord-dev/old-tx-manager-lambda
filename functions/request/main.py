# Method for handling all tX-Manager requests

from __future__ import print_function

import boto3
import json
import requests
import re
#import uuid

import gogs_client

from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key, Attr

class TXManager(object):
    
    def __init__(self, data=None):
        self.data = data
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
        data = self.data

        if 'gogs_url' not in data or not data['gogs_url']:
            raise Exception('"gogs_url" not in payload')
        if 'user_token' not in data or not data['user_token']:
            raise Exception('"user_token" not in payload')
        
        gogs_url = data['gogs_url']
        gogs_api = gogs_client.GogsApi(gogs_url)
        success = gogs_api.valid_authentication(gogs_client.Token(data['user_token']))
        if not success:
            raise Exception('"user_token" invalid, needs to be a valid Gogs user at {0}'.format(data['gogs_url']))
        if 'username' in data and data['username']:
            return data['username']
        else:
            return data['user_token']

    def list_endpoints(self):
        return {
            "version": "1",
            "links": [
                {
                    "href": "https://{0}/tx/job".format(self.data['api_bucket']),
                    "rel": "list",
                    "method": "GET"
                },
                {
                    "href": "https://{0}/tx/job".format(self.data['api_bucket']),
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
                raise Exception('"{0}" not in payload'.format(field))
        
        for field in list_fields:
            if field in module:
                if not isinstance(module[field], list):
                    module[field] = [module[field]]
        
        for field, value in enumerate(module):
            if field not in list_fields and isinstance(value, list):
                raise Exception('"{0}" cannot be a list'.format(field))
        
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

        # lambda_func_name = module['name']
        # AWS_LAMBDA_API_ID = '7X97xCLPDE16Jep5Zv85N6zy28wcQfJz79E2H3ln'
        # # of 'tx-manager_api_key'
        # # or fkcr7r4dz9
        # # or 7X97xCLPDE16Jep5Zv85N6zy28wcQfJz79E2H3ln
        # AWS_REGION = 'us-west-2'
        #
        # api_client = boto3.client('apigateway')
        # aws_lambda = boto3.client('lambda')
        #
        # ## create resource
        # resource_resp = api_client.create_resource(
        #     restApiId=AWS_LAMBDA_API_ID,
        #     parentId='foo', # resource id for the Base API path
        #     pathPart=lambda_func_name
        # )
        #
        # ## create POST method
        # put_method_resp = api_client.put_method(
        #     restApiId=AWS_LAMBDA_API_ID,
        #     resourceId=resource_resp['id'],
        #     httpMethod="POST",
        #     authorizationType="NONE",
        #     apiKeyRequired=True,
        # )
        #
        # lambda_version = aws_lambda.meta.service_model.api_version
        #
        # uri_data = {
        #     "aws-region": AWS_REGION,
        #     "api-version": lambda_version,
        #     "aws-acct-id": "xyzABC",
        #     "lambda-function-name": lambda_func_name,
        # }
        #
        # uri = "arn:aws:apigateway:{aws-region}:lambda:path/{api-version}/functions/arn:aws:lambda:{aws-region}:{aws-acct-id}:function:{lambda-function-name}/invocations".format(**uri_data)
        #
        # ## create integration
        # integration_resp = api_client.put_integration(
        #     restApiId=AWS_LAMBDA_API_ID,
        #     resourceId=resource_resp['id'],
        #     httpMethod="POST",
        #     type="AWS",
        #     integrationHttpMethod="POST",
        #     uri=uri,
        # )
        #
        # api_client.put_integration_response(
        #     restApiId=AWS_LAMBDA_API_ID,
        #     resourceId=resource_resp['id'],
        #     httpMethod="POST",
        #     statusCode="200",
        #     selectionPattern=".*"
        # )
        #
        # ## create POST method response
        # api_client.put_method_response(
        #     restApiId=AWS_LAMBDA_API_ID,
        #     resourceId=resource_resp['id'],
        #     httpMethod="POST",
        #     statusCode="200",
        # )
        #
        # uri_data['aws-api-id'] = AWS_LAMBDA_API_ID
        # source_arn = "arn:aws:execute-api:{aws-region}:{aws-acct-id}:{aws-api-id}/*/POST/{lambda-function-name}".format(**uri_data)
        #
        # aws_lambda.add_permission(
        #     FunctionName=lambda_func_name,
        #     StatementId=uuid.uuid4().hex,
        #     Action="lambda:InvokeFunction",
        #     Principal="apigateway.amazonaws.com",
        #     SourceArn=source_arn
        # )
        #
        # # state 'your stage name' was already created via API Gateway GUI
        # api_client.create_deployment(
        #     restApiId=AWS_LAMBDA_API_ID,
        #     stageName="your stage name",
        # )

        return module

    def setup_job(self):
        dynamodb = boto3.resource('dynamodb')
        data = self.data
        
        user = self.get_user()

        if 'cdn_bucket' not in data:
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
        
        job_id = data['job_id']  # The JOB ID will simply be this Lambda instance's unique request ID in the handle function
        output_file = 'tx/job/{0}.zip'.format(job_id) # All conversions must result in a ZIP of the converted file(s)
        output_url = 'https://{0}/{1}'.format(data["cdn_bucket"], output_file)

        identifier = job_id
        if 'identifier' in data:
            identifier = data['identifier']

        created_at = datetime.utcnow()
        expires_at = created_at + timedelta(days=1)
        eta = created_at + timedelta(minutes=2)

        created_at_timestamp = created_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        expires_at_timestamp = expires_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        eta_timestamp = eta.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Info to store to DynamoDB tx-job table
        job = {
            "job_id": job_id,
            "user": user,
            "identifier": identifier,
            "source": data["source"],
            "resource_type": data["resource_type"],
            "input_format": data["input_format"],
            "output_format": data["output_format"],
            "created_at": created_at_timestamp,
            "eta": eta_timestamp,
            "output": output_url,
            "cdn_bucket": data["cdn_bucket"],
            "cdn_file": output_file,
            "door43_bucket": data["door43_bucket"],
            "output_expiration": expires_at_timestamp,
            "job_status": "requested",
            "success": None,
            "deployed": False,
            "links": {
                "href": "https://{0}/job/{1}".format(data['api_bucket'], job_id),
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
                    "href": "https://{0}/job".format(data['api_bucket']),
                    "rel": "list",
                    "method": "GET"
                },
                {
                    "href": "https://{0}/job/{1}".format(data['api_bucket'], job['job_id']),
                    "rel": "create",
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

    def start_job(self):
        dynamodb = boto3.resource('dynamodb')
        job_table = dynamodb.Table('tx-job')

        job = self.data

        started_at_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        self.log_message("Started job {0} at {1}".format(job['job_id'], started_at_timestamp))

        try:
            job_table.update_item(
                Key={
                    'job_id': job['job_id'],
                },
                UpdateExpression="set started_at = :started_at, job_status = :job_status",
                ExpressionAttributeValues={
                    ':started_at': started_at_timestamp,
                    ':job_status': 'started'
                }
            )
            print('Updated job in tx-job table with started_at = {0}'.format(started_at_timestamp))

            job['started_at'] = started_at_timestamp
            job['job_status'] = 'started'

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

            job['convert_function'] = module['name']

            payload = {
                'data': {
                    'job': job,
                }
            }
            print("Payload to {0}:".format(module['name']))
            print(payload)

            self.log_message('Telling module {0} to convert {1} and put at {2}'.format(module['name'], job['source'], job['output']))
            lambda_client = boto3.client('lambda')
            response = lambda_client.invoke(
                FunctionName=module['name'],
                Payload=json.dumps(payload)
            )
            response = json.loads(response['Payload'].read())
            print("Response payload from {0}:".format(module['name']))
            print(response)

            if 'errorMessage' in response:
                self.errors.append("{0}: {1}".format(module['name'], response['errorMessage']))
            else:
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
            job_status = "failed"
            message = "Conversion failed"
        elif len(self.warnings) > 0:
            success = True
            job_status = "warnings"
            message = "Conversion successful with warnings"
        else:
            success = True
            job_status = "success"
            message = "Conversion successful"

        self.log_message(message)

        ended_at_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        self.log_message('Finished job {0} at {1}'.format(job['job_id'], ended_at_timestamp))

        response = job_table.update_item(
            Key={
                'job_id': job['job_id'],
            },
            UpdateExpression="set ended_at = :ended_at, success = :success, job_status = :job_status, message = :message, logs = :logs, errors = :errors, warnings = :warnings, deployed = :deployed",
            ExpressionAttributeValues={
                ':ended_at': ended_at_timestamp,
                ':success': success,
                ':job_status': job_status,
                ':message': message,
                ':logs': json.dumps(self.log),
                ':errors': json.dumps(self.errors),
                ':warnings': json.dumps(self.warnings),
                ':deployed': False
            }
        )
        print("Updated tx-job with ended_timestamp, success, job_status, message, logs, errors and warnings.")

        response = job_table.get_item(
            Key={
                'job_id': job['job_id']
            }
        )
        job = response['Item']

        response = {
            "job_id": job["job_id"],
            "identifier": job["identifier"],
            "success": success,
            "status": job_status,
            "message": message,
            "output": job["output"],
            "ouput_expiration": job["output_expiration"],
            "log": self.log,
            "warnings": self.warnings,
            "errors": self.errors,
            "created_at": job["created_at"],
            "started_at": job["started_at"],
            "ended_at": ended_at_timestamp
        }

        if 'callback' in job and job['callback'] and job['callback'].startswith('http'):
            callback_url = job['callback']
            headers = {"content-type": "application/json"}
            print('Making callback to {0} with payload:'.format(callback_url))
            print(response)
            response = requests.post(callback_url, json=response, headers=headers)
            print('finished.')

        return response


def handle(event, context):
#    print("Received event: " + json.dumps(event, indent=2))

    ret = {}
#    try:
    if True:
        # First see if this is a call from a DynamoDB Stream to start a job:
        if 'Records' in event:
            print("------------PROCESSING DB STREAM---------------------")
            jobs_converted = 0
            jobs_failed = 0
            for record in event['Records']:
                if record['eventName'] == 'INSERT' and 'job_id' in record['dynamodb']['Keys']:
                    print("DynamoDB Record: " + json.dumps(record['dynamodb'], indent=2))
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
                    print("job_id: {0}".format(job_id))
                    #print("RESPONSE:")
                    #print(response)
                    if response and 'Item' in response:
                        job = response['Item']
                        # Only start the job if a started timestamp hasn't been set
                        if 'started_at' not in job or not job['started_at']:
                            manager = TXManager(job)
                            manager.start_job()
                            if len(manager.errors) > 0:
                                jobs_converted += 1
                            else:
                                jobs_failed += 1
            if jobs_converted > 0:
                if jobs_failed > 0:
                    ret = {
                        'success': True,
                        'message': 'Succeeded to converted {} job(s). Failed to convert {1} job(s).'.format(jobs_converted, jobs_failed)
                    }
                else:
                    ret = {
                        'success': True,
                        'message': 'Succeeded to converted {} job(s).'.format(jobs_converted)
                    }
            if jobs_failed > 0:
                ret = {
                    'success': False,
                    'message': 'Failed to convert {} job(s).'.format(jobs_failed)
                }
            print("------------END PROCESSING DB STREAM---------------------")
        # It isn't a DynamoDB Stream call, so we look at this as an API Gateway call
        else:
            path = "" # path from the API Gateway
            if 'path' in event:
                path = re.sub(ur'\{[^\}]*\}', ur'', event['path']).strip('/')

            # Get all params, both POST and GET and JSON from the request event
            data = {}
            if 'data' in event and isinstance(event['data'], dict):
                data = event['data']
            if 'body-json' in event and event['body-json'] and isinstance(event['body-json'], dict):
                data.update(event['body-json'])
            if 'vars' in event and isinstance(event['vars'], dict):
                data.update(event['vars'])

            if not path:
                return TXManager(data).list_endpoints()
            elif path == 'module':
                return TXManager(data).register_module()
            elif path == 'job':
                if 'source' in data and 'job_id' not in data:
                    data['job_id'] = context.aws_request_id
                    return TXManager(data).setup_job()
                else:
                    return TXManager(data).list_jobs()
            else:
                raise Exception('Invalid action')
#    except Exception as e:
#        print(e)
#        print(e.message)
#        e.message = 'Bad request: {0}'.format(e.message)
#        raise e
    return ret
