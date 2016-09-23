# Method for handling all tX-Manager requests

from __future__ import print_function

from tx_manager.tx_manager import TxManager


def handle(event, context):
    try:
        # Get all params, both POST and GET and JSON from the request event
        job = {}
        if 'data' in event and isinstance(event['data'], dict):
            job = event['data']
        if 'body-json' in event and event['body-json'] and isinstance(event['body-json'], dict):
            job.update(event['body-json'])

        env_vars = {}
        if 'vars' in event and isinstance(event['vars'], dict):
            env_vars = event['vars']

        # if 'source' is given, and no job_id, that means to setup a new job for conversion
        if 'source' in job and 'job_id' not in job:
            job['job_id'] = context.aws_request_id
            return TxManager(api_url=vars).setup_job(job)
        # Else we just list all jobs based on the given query data
        else:
            return TxManager(env_vars).list_jobs(job)
    except Exception as e:
        print(e)
        print(e.message)
        e.message = 'Bad request: {0}'.format(e.message)
        raise e
