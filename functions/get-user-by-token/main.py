# Current Dummy function to take 'aaaaaaa' as the token and return 'rich' as the user
# Wil be hooked up with python-gogs-client to actually query git.door43.org

from __future__ import print_function

def handle(event, ctx):
    if 'user_token' not in event or not event['user_token']:
        return {"error": "user_token not given"}

    if event['user_token'] != 'aaaaaa':
        return {"error": "user_token invalid"}

    return {
        "success": "valid user",
        "user": "rich",
        "full_name": "Rich M"
    }
