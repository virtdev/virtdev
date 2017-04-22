# parent.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from lib.loader import Loader
from lib.attributes import ATTR_PARENT

class Parent(object):
	def __init__(self, uid):
		self._parent = {}
		self._loader = Loader(uid)

	def _get(self, name):
		parent = self._loader.get_attr(name, ATTR_PARENT, str)
		if parent != None:
			self._parent[name] = parent
			return parent

	def get(self, name):
		if self._parent.has_key(name):
			ret = self._parent.get(name)
			if ret != None:
				return ret
		return self._get(name)

	def remove(self, name):
		if self._parent.has_key(name):
			del self._parent[name]
