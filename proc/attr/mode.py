# mode.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from lib.log import log_debug
from conf.log import LOG_MODE
from lib.loader import Loader
from lib.attributes import ATTR_MODE

class Mode(object):
	def __init__(self, uid):
		self._mode = {}
		self._loader = Loader(uid)

	def _log(self, text):
		if LOG_MODE:
			log_debug(self, text)

	def _get(self, name):
		mode = self._loader.get_attr(name, ATTR_MODE, int)
		if mode != None:
			self._mode[name] = mode
			self._log('name=%s, mode=%s' % (str(name), str(mode)))
			return mode

	def get(self, name):
		if self._mode.has_key(name):
			ret = self._mode.get(name)
			if ret != None:
				return ret
		return self._get(name)

	def remove(self, name):
		if self._mode.has_key(name):
			del self._mode[name]
