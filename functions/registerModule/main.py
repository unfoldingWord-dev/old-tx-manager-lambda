# Method for handling the registration of conversion modules

from __future__ import print_function

from tx_manager.tx_manager import TxManager


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

        return TxManager(data).register_module()
    except Exception as e:
        print(e)
        print(e.message)
        e.message = 'Bad request: {0}'.format(e.message)
        raise e
