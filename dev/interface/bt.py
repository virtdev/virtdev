# bt.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
import lo
import bluetooth
from dev.udi import UDI
from conf.virtdev import LO
from lib.util import get_conf_path
from lib.bt import BluetoothSocket

PIN = '1234'
DEVICE_MAX = 32

class Bluetooth(UDI):
	def _get_devices(self):
		cnt = 0
		device_list = []
		path = os.path.join(get_conf_path(), 'devices')
		if os.path.exists(path):
			with open(path, 'r') as f:
				while True:
					buf = f.readline().strip()
					if buf:
						device_list.append(buf)
						cnt += 1
						if cnt == DEVICE_MAX:
							break
					else:
						break
		return device_list

	def scan(self):
		device_list = []
		devices = self._get_devices()
		if not devices:
			return device_list
		bt_devices = bluetooth.discover_devices()
		if bt_devices:
			for i in bt_devices:
				if i in devices:
					device_list.append(i)
		return device_list

	def _init(self, device):
		os.system('bluez-test-device remove %s' % device)
		os.system('echo %s | bluez-simple-agent hci0 %s' % (PIN, device))

	def connect(self, device):
		self._init(device)
		if LO:
			sock = lo.connect(lo.device_name('Controller', device))
			if sock:
				return (sock, True)
		return (BluetoothSocket(device), False)
