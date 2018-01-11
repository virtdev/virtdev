# signaling.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import uuid
from lib import codec
from lib.util import call
from lib.log import log_err
from hash_ring import HashRing
from operations import OP_EXIST
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
from threading import Thread, Event
from conf.virtdev import SIGNALING_PORT, SIGNALING_SERVERS

QOS = 2       # -1: No QOS, 0: At Most Once, 1: At Least Once, 2: Exactly Once
TIMEOUT = 30  # sec
KEEPLIVE = 30 # sec
RETRY_MAX = 3
RETRY_INTERVAL = 0

REPLY_CMD = "reply"

def get_broker(addr):
    ring = HashRing(SIGNALING_SERVERS)
    return ring.get_node(addr)

class Signaling(Thread):
    def run(self):
        call('mosquitto', '-d', '-p', str(SIGNALING_PORT))

class SignalingClient(object):
    def __init__(self, addr, handle):
        if SIGNALING_SERVERS:
            self._addr = addr
            self._handle = handle
            self._client = mqtt.Client()
            self._client.on_connect = self.on_connect
            self._client.on_message = self.on_message
            self._client.connect(get_broker(addr), SIGNALING_PORT, KEEPLIVE)
            self._client.loop_start()
        else:
            self._addr = None
            self._handle = None
            self._client = None

    def on_connect(self, client, userdata, flags, rc):
        if QOS >= 0:
            self._client.subscribe(self._addr, qos=QOS)
        else:
            self._client.subscribe(self._addr)

    def on_message(self, client, userdata, msg):
        self._handle(msg.payload)

    def disconnect(self):
        try:
            self._client.unsubscribe(self._addr)
        except:
            pass
        finally:
            self._client.disconnect()
            self._client.loop_stop(force=True)

def send(uid, addr, req, token):
    broker = get_broker(addr)
    buf = codec.encode(req, token, uid)
    if QOS >= 0:
        publish.single(addr, buf, hostname=broker, port=SIGNALING_PORT, qos=QOS)
    else:
        publish.single(addr, buf, hostname=broker, port=SIGNALING_PORT)

def reply(addr, broker):
    if QOS >= 0:
        publish.single(addr, REPLY_CMD, hostname=broker, port=SIGNALING_PORT, qos=QOS)
    else:
        publish.single(addr, REPLY_CMD, hostname=broker, port=SIGNALING_PORT)

def _gen_addr():
    return uuid.uuid4().hex

def exist(uid, addr, token):
    hdl_event = Event()
    def handle(cmd):
        if str(cmd) == REPLY_CMD:
            hdl_event.set()
        else:
            log_err(self, "receive an invalid command %s" % str(cmd))
    src = _gen_addr()
    broker = get_broker(src)
    client = SignalingClient(src, handle)
    req = {'op':OP_EXIST, 'args':{'dest':addr, 'src':src, 'broker':broker}}
    cnt = RETRY_MAX
    while cnt >= 0:
        try:
            send(uid, addr, req, token)
            hdl_event.wait(TIMEOUT)
            if hdl_event.is_set():
                return True
        except:
            pass
        cnt -= 1
        if cnt >= 0:
            time.sleep(RETRY_INTERVAL)
    client.disconnect()
