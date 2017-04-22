# udi.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import time
import copy
from lib import io
from udo import UDO
from lib.cmd import cmd_mount
from threading import Lock, Thread
from lib.log import log_get, log_err
from lib.util import get_name, device_info
from lib.modes import MODE_CLONE, MODE_VIRT
from multiprocessing.pool import ThreadPool

PAIR_INTERVAL = 7 # seconds
SCAN_INTERVAL = 7 # seconds
MOUNT_TIMEOUT = 30 # seconds

class UDI(object):
	def __init__(self, uid, core):
		self._thread = None
		self._lock = Lock()
		self._devices = {}
		self._core = core
		self._uid = uid
		self.setup()

	def setup(self):
		pass

	def scan(self):
		pass

	def connect(self, device):
		pass

	def get_uid(self):
		return self._uid

	def get_name(self, parent, child=None):
		return get_name(self._uid, parent, child)

	def get_mode(self, device):
		return 0

	def _create_device(self, info, local, index=None):
		if not info.has_key('type'):
			log_err(self, 'failed to create device')
			raise Exception(log_get(self, 'failed to create device'))

		device = UDO(local=local)
		if index != None:
			device.set_index(int(index))

		device.set_type(str(info['type']))

		if info.get('freq'):
			device.set_freq(float(info['freq']))

		if info.get('mode'):
			mode = int(info['mode'])
			device.set_mode(mode)

		if info.get('spec'):
			device.set_spec(dict(info['spec']))

		return device

	def _get_children(self, parent, info, local):
		devices = {}
		try:
			for i in info:
				device = self._create_device(info[i], local, i)
				child = self.get_name(parent, i)
				devices.update({child:device})
		except:
			log_err(self, 'failed to get children')
			return
		for i in devices:
			devices[i].mount(self._uid, i, self._core)
		return devices

	def _get_info(self, sock, local):
		io.put(sock, cmd_mount(), local=local)
		buf = io.get(sock, local=local)
		if buf:
			return device_info(buf)

	def _mount(self, sock, local, device, init):
		info = self._get_info(sock, local)
		if not info:
			log_err(self, 'failed to mount')
			return
		name = self.get_name(device)
		if info.has_key('None'):
			info = info['None']
			if not info:
				log_err(self, 'failed to mount')
				return
			if local:
				parent = self._create_device(info, local)
			else:
				log_err(self, 'failed to mount, invalid device')
				return
		else:
			children = self._get_children(name, info, local)
			if not children:
				log_err(self, 'failed to mount, no device')
				return
			parent = UDO(children, local)
		if self.get_mode(device) & MODE_CLONE:
			sock = None
		parent.mount(self._uid, name, self._core, sock=sock, init=init)
		self._devices.update({name:parent})
		return name

	def _proc(self, target, args, timeout):
		pool = ThreadPool(processes=1)
		result = pool.apply_async(target, args=args)
		pool.close()
		try:
			return result.get(timeout=timeout)
		finally:
			pool.join()

	def _create(self, device, init=True):
		try:
			sock, local = self._proc(self.connect, (device,), PAIR_INTERVAL)
		except:
			return
		if sock:
			name = self._proc(self._mount, (sock, local, device, init), MOUNT_TIMEOUT)
			if not name:
				log_err(self, 'failed to create')
				sock.close()
			else:
				mode = self.get_mode(device)
				if mode & MODE_CLONE or mode & MODE_VIRT:
					sock.close()
				return name

	def create(self, device, init=True):
		return self._create(device, init)

	def find(self, name):
		devices = copy.copy(self._devices)
		for i in devices:
			if devices[i].find(name):
				return devices[i]

	def _start(self):
		while True:
			devices = self.scan()
			if devices:
				for device in devices:
					self._create(device)
			time.sleep(SCAN_INTERVAL)

	def start(self):
		self._thread = Thread(target=self._start)
		self._thread.start()
