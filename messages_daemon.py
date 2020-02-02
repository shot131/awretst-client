from daemoniker import Daemonizer

with Daemonizer() as (is_setup, daemonizer):
    pid_file = './awrtest_messages_daemon.pid'
    daemonizer(pid_file)

# In daemon
from persistqueue import Queue
import requests
from requests.exceptions import HTTPError
import random
import json
import time
from datetime import datetime, timedelta

q = Queue("messages")


def send_message(message):
    sent = False
    try:
        response = requests.post(
            'https://213.219.214.58/api/message/create/',
            verify='server.crt',
            cert='client.crt',
            data={
                'data': json.dumps({
                    'id': 'FG3f4h7t973h4g5b3498h',
                    'time': message['time'],
                    'value': message['value'],
                })
            }
        )
        response.raise_for_status()
    except HTTPError as http_err:
        print(http_err)
        pass
    except Exception as err:
        print(err)
        pass
    else:
        json_response = response.json()
        if 'success' in json_response and json_response['success'] is True:
            sent = True
    return sent


def process_messages():
    while q.qsize() > 0:
        message = q.get()
        sent = send_message(message)
        if sent:
            q.task_done()
        else:
            q.put(message)
            break


queue_delta = timedelta(minutes=5)
queue_timer = datetime.now()
send_delta = timedelta(minutes=1)
send_timer = datetime.now()
while True:
    if queue_timer <= datetime.now():
        q.put({
            'time': int(datetime.now().timestamp()),
            'value': random.randrange(1, 100000),
        })
        queue_timer = datetime.now() + queue_delta
    if send_timer <= datetime.now():
        process_messages()
        send_timer = datetime.now() + send_delta
    time.sleep(1)
