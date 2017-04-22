# timeout.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from lib.loader import Loader
from lib.attributes import ATTR_TIMEOUT

class Timeout(object):
	def __init__(self, uid):
		self._timeout = {}
		self._loader = Loader(uid)

	def _get(self, name):
		timeout = self._loader.get_attr(name, ATTR_TIMEOUT, float)
		if timeout != None:
			self._timeout[name] = timeout
			return timeout

	def get(self, name):
		if self._timeout.has_key(name):
			ret = self._timeout.get(name)
			if ret != None:
				return ret
		return self._get(name)

	def remove(self, name):
		if self._timeout.has_key(name):
			del self._timeout[name]
