#!/usr/bin/env python3
import sys
import zmq
import time

host='localhost'

context = zmq.Context()
def connect(topics):
    # Socket to talk to server
    socket = context.socket(zmq.SUB)
    socket.connect ("tcp://%s:%s" % (host, 14000))
    for topic in topics:
        socket.setsockopt_string(zmq.SUBSCRIBE, topic)
    return socket


def sub(topics):
    control = context.socket(zmq.DEALER)
    control.connect("tcp://%s:%s" % (host, 14001))
    for topic in topics:
        control.send_multipart([b"SUB", topic.encode('utf-8')]) #needs to be done periodically

    #TODO: have gateway send a response to SUB, if no response, recreate sockets.

def main():
    topics = sys.argv[1:]

    socket = connect(topics)

    last_sub = 0
    while True:
        if time.time() - last_sub > 10:
            sub(topics)
            last_sub = time.time()

        if socket.poll(1000):
            topic, who, messagedata = socket.recv_multipart()
            print(topic, who, messagedata)

if __name__ == "__main__":
    main()
