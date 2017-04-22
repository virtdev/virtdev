# usb.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
import lo
from dev.udi import UDI
from conf.virtdev import LO
from lib.usb import USBSocket

DEV = '/dev'

class USBSerial(UDI):
	def setup(self):
		self._usb = {}

	def scan(self):
		devices = filter(lambda x:x.startswith('ttyACM'), os.listdir(DEV))
		if devices:
			names = map(lambda x: os.path.join(DEV, x), devices)
			return filter(lambda x: x not in self._usb, names)

	def connect(self, device):
		ret = None
		if LO:
			sock = lo.connect(lo.device_name('Controller', device))
			if sock:
				ret = (sock, True)
		if not ret:
			ret = (USBSocket(device), False)
		self._usb.update({device:None})
		return ret
