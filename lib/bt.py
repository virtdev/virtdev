# bt.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import time
import bluetooth
from conf.defaults import BT_PORT

TIMEOUT = 0.5
WAIT_TIME = 0.5 # seconds

class BluetoothSocket(object):
	def __init__(self, name):
		self._socket = bluetooth.BluetoothSocket(bluetooth.RFCOMM)
		self._socket.connect((name, BT_PORT))
		self._socket.settimeout(TIMEOUT)

	def sendall(self, buf):
		if self._socket:
			self._socket.sendall(buf)

	def send(self, buf):
		if self._socket:
			self._socket.send(buf)

	def recv(self, length):
		if self._socket:
			return self._socket.recv(length)

	def close(self):
		try:
			if self._socket:
				self._socket.close()
				self._socket = None
				time.sleep(WAIT_TIME)
		except:
			pass

	def __del__(self):
		self.close()
