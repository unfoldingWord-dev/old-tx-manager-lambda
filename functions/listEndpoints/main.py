# Method for handling the registration of conversion modules

from __future__ import print_function

from tx_manager.tx_manager import TxManager


def handle(event, context):
    if True:
#    try:
        env_vars = {}
        if 'vars' in event and isinstance(event['vars'], dict):
            env_vars = event['vars']
        print(env_vars)

        return TxManager(**env_vars).list_endpoints()
#    except Exception as e:
        print(e)
        print(e.message)
        e.message = 'Bad request: {0}'.format(e.message)
        raise e
