# Method for handling all tX-Manager requests

from __future__ import print_function

from manager_tools import txmanager


def handle(event, context):
    try:
        # Get all params, both POST and GET and JSON from the request event
        data = {}
        if 'data' in event and isinstance(event['data'], dict):
            data = event['data']
        if 'body-json' in event and event['body-json'] and isinstance(event['body-json'], dict):
            data.update(event['body-json'])
        if 'vars' in event and isinstance(event['vars'], dict):
            data.update(event['vars'])

        if 'source' in data and 'job_id' not in data:
            data['job_id'] = context.aws_request_id
            return txmanager.TXManager(data).setup_job()
        else:
            return txmanager.TXManager(data).list_jobs()
    except Exception as e:
        print(e)
        print(e.message)
        e.message = 'Bad request: {0}'.format(e.message)
        raise e
