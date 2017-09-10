#!/usr/bin/env python3
import sys
import zmq
import time
import json
import os

HEADER="#fields indicator indicator_type meta.source meta.desc\n".replace(" ", "\t")


context = zmq.Context()
def connect(host, topics):
    # Socket to talk to server
    socket = context.socket(zmq.SUB)
    socket.connect ("tcp://%s:%s" % (host, 14000))
    for topic in topics:
        socket.setsockopt(zmq.SUBSCRIBE, topic.encode('utf-8'))
    return socket


def sub(host, topics):
    control = context.socket(zmq.DEALER)
    control.connect("tcp://%s:%s" % (host, 14001))
    for topic in topics:
        control.send_multipart([b"SUB", topic.encode('utf-8')]) #needs to be done periodically

    control.close()
    #TODO: have gateway send a response to SUB, if no response, recreate sockets.

def format_rec(messagedata):
    rec = json.loads(messagedata)
    out = "{description}".format(**rec)
    return rec['indicator'], out

def format_line(indicator, source, description):
    itype ='Intel::ADDR'
    source = "p2p." + source
    fields = indicator, itype, source, description
    return '\t'.join(fields) + '\n'

def main():
    topics = sys.argv[1].split(",")
    try:
        destination_file = sys.argv[2]
    except IndexError:
        destination_file = 'bro.intel'

    try:
        host = sys.argv[3]
    except IndexError:
        host = 'localhost'

    socket = connect(host, topics)

    f = open(destination_file, 'w')
    f.write(HEADER)
    f.flush()

    seen = set()
    last_sub = 0
    while True:
        if time.time() - last_sub > 10:
            sub(host, topics)
            last_sub = time.time()

        if socket.poll(1000):
            msg = socket.recv_multipart()
            msg = [ m.decode('utf-8') for m in msg ]
            topic, who, messagedata = msg
            indicator, msg = format_rec(messagedata)
            if indicator in seen:
                continue
            seen.add(indicator)
            line = format_line(indicator, who, msg)
            sys.stdout.write(line)
            f.write(line)
            f.flush()

if __name__ == "__main__":
    main()
