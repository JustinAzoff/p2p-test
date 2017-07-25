#!/usr/bin/env python3
import sys
import zmq
import time
import json
import os
import itertools

HOST = 'localhost'
TOPIC = 'ticker'

context = zmq.Context()

def tick(host, topic):
    control = context.socket(zmq.DEALER)
    control.connect("tcp://%s:%s" % (host, 14001))
    topic = topic.encode('utf-8')

    for i in itertools.count():
        msg = str(i).encode('utf-8')
        control.send_multipart([b"PUB", topic, msg])
        time.sleep(1)
    control.close()

if __name__ == "__main__":
    tick(HOST, TOPIC)
