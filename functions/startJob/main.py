# Method for handling the starting of jobs that have been queued in the tx-job table

from __future__ import print_function

from manager_tools import txmanager
from manager_tools import db_handler

def handle(event, context):
    print("------------PROCESSING DB STREAM---------------------")
    for record in event['Records']:
        try:
            if record['eventName'] == 'INSERT' and 'job_id' in record['dynamodb']['Keys']:
                print(record['eventID'])
                print(record['eventName'])
                # print("DynamoDB Record: " + json.dumps(record['dynamodb'], indent=2))
                job_id = record['dynamodb']['Keys']['job_id']['S']
        except Exception:
            pass
        print("------------END PROCESSING DB STREAM---------------------")
