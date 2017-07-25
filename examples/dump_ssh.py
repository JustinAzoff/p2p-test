#!/usr/bin/env python3
from __future__ import print_function
import sys
import zmq
import time
import json

host='localhost'

context = zmq.Context()
def connect(topics):
    # Socket to talk to server
    socket = context.socket(zmq.SUB)
    socket.connect ("tcp://%s:%s" % (host, 14000))
    for topic in topics:
        socket.setsockopt(zmq.SUBSCRIBE, topic.encode('utf-8'))
    return socket


def sub(topics):
    control = context.socket(zmq.DEALER)
    control.connect("tcp://%s:%s" % (host, 14001))
    for topic in topics:
        control.send_multipart([b"SUB", topic.encode('utf-8')]) #needs to be done periodically

    #TODO: have gateway send a response to SUB, if no response, recreate sockets.

def show_ssh(topic, who, messagedata):
    prefix = "{}:".format(who)
    rec = json.loads(messagedata)
    out = "{description} {indicator} -> {dest}:{dest_portlist}".format(**rec)
    if 'additional_data' in rec and 'duser' in rec['additional_data']:
        if 'fingerprint' in rec['additional_data']:
            rec['additional_data']['password'] = rec['additional_data']['fingerprint']
        if 'password' in rec['additional_data']:
            out += " {duser}:{password} using {client_version}".format(**rec['additional_data'])
    print(prefix, out)

def main():
    topics = ["ssh"]

    socket = connect(topics)

    last_sub = 0
    while True:
        if time.time() - last_sub > 10:
            sub(topics)
            last_sub = time.time()

        if socket.poll(1000):
            msg = socket.recv_multipart()
            msg = [ m.decode('utf-8') for m in msg ]
            topic, who, messagedata = msg
            show_ssh(topic, who, messagedata)

if __name__ == "__main__":
    main()
