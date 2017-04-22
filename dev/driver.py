# driver.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
import imp
from lib.util import get_dir
from lib.modes import MODE_IV, MODE_POLL, MODE_TRIG, MODE_CTRL

FREQ_MIN = 0.1 # HZ
FREQ_MAX = 100 # HZ

def need_freq(mode):
	return mode & MODE_POLL or (mode & MODE_TRIG and mode & MODE_CTRL)

def wrapper(func):
	def _wrapper(*args, **kwargs):
		reserve = {}
		if kwargs.has_key('name'):
			reserve.update({'name':kwargs['name']})
		if kwargs.has_key('timer'):
			reserve.update({'timer':kwargs['timer']})
		ret = func(*args, **kwargs)
		if ret and type(ret) == dict:
			ret.update(reserve)
			return ret
	return _wrapper

def load_driver(typ, name=None):
	try:
		driver_dir = os.path.join(get_dir(), 'drivers', typ.lower())
		path = os.path.join(driver_dir, '__init__.py')
		module = imp.load_source(typ, path)
		if module and hasattr(module, typ):
			driver = getattr(module, typ)
			if driver:
				return driver(name=name)
	except:
		pass

class Driver(object):
	def __init__(self, name=None, mode=MODE_IV, freq=None, spec=None):
		self.__name = name
		self.__mode = mode
		self.__spec = spec
		self.__freq = freq
		self.__index = None
		if not freq:
			if need_freq(self.__mode):
				self.__freq = FREQ_MIN
		else:
			if freq > FREQ_MAX:
				self.__freq = FREQ_MAX
			if freq < FREQ_MIN:
				self.__freq = FREQ_MIN

	def __str__(self):
		return self.__class__.__name__

	def setup(self):
		pass

	def open(self):
		pass

	def close(self):
		pass

	def get(self):
		pass

	def put(self, *args, **kwargs):
		pass

	def set_index(self, index):
		self.__index = index

	def get_type(self):
		return str(self)

	def get_index(self):
		return self.__index

	def get_profile(self):
		profile = {'type':self.get_type()}
		spec = self.get_spec()
		if spec:
			profile.update({'spec':spec})
		return profile

	def get_freq(self):
		return self.__freq

	def get_spec(self):
		return self.__spec

	def get_mode(self):
		return self.__mode

	def get_name(self):
		return self.__name

	def get_info(self):
		info = {'type':self.get_type(), 'mode':self.get_mode()}
		freq = self.get_freq()
		if freq:
			info.update({'freq':freq})
		spec = self.get_spec()
		if spec:
			info.update({'spec':spec})
		return str({'None':info})
