# sig.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import uuid
import codec
from lib.util import call
from hash_ring import HashRing
import paho.mqtt.client as mqtt
from operations import OP_EXIST
import paho.mqtt.publish as publish
from threading import Thread, Event
from conf.virtdev import SIGNAL_PORT, SIGNAL_SERVERS

TIMEOUT = 5 # sec
KEEPLIVE = 60 # sec

def get_broker(addr):
	ring = HashRing(SIGNAL_SERVERS)
	return ring.get_node(addr)

class Signal(Thread):
	def run(self):
		call('mosquitto', '-p', str(SIGNAL_PORT))

class SignalClient(object):
	def __init__(self, addr, handle):
		self._addr = addr
		self._handle = handle
		broker = get_broker(addr)
		self._client = mqtt.Client()
		self._client.on_connect = self.on_connect
		self._client.on_message = self.on_message
		self._client.connect(broker, SIGNAL_PORT, KEEPLIVE)
		self._client.loop_start()

	def on_connect(self, client, userdata, flags, rc):
		self._client.subscribe(self._addr)

	def on_message(self, client, userdata, msg):
		self._handle(msg.payload)

	def disconnect(self):
		try:
			self._client.unsubscribe(self._addr)
		finally:
			self._client.disconnect()
			self._client.loop_stop(force=True)

def send(uid, addr, req, token):
	broker = get_broker(addr)
	buf = codec.encode(req, token, uid)
	publish.single(addr, buf, hostname=broker, port=SIGNAL_PORT)

def reply(addr, broker):
	publish.single(addr, hostname=broker, port=SIGNAL_PORT)

def _gen_addr():
	return uuid.uuid4().hex

def exist(uid, addr, token):
	ev = Event()
	client = None
	def handle(_):
		try:
			if client:
				client.disconnect()
		finally:
			ev.set()
	try:
		src = _gen_addr()
		broker = get_broker(src)
		client = SignalClient(src, handle)
		req = {'op':OP_EXIST, 'args':{'dest':addr, 'src':src, 'broker':broker}}
		send(uid, addr, req, token)
		ev.wait(TIMEOUT)
	except:
		pass
	finally:
		if ev.is_set():
			return True
		else:
			if client:
				client.disconnect()
