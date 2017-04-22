# loader.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
import json
from fields import FIELD_ATTR
from attributes import ATTR_PROFILE
from util import unicode2str, get_mnt_path

BUF_LEN = 4096

class Loader(object):
	def __init__(self, uid):
		self._uid = uid
		self._mnt = get_mnt_path(self._uid)

	def _get_path(self, name, attr):
		return os.path.join(self._mnt, FIELD_ATTR, name, attr)

	def _read(self, name, attr):
		path = self._get_path(name, attr)
		try:
			fd = os.open(path, os.O_RDONLY)
			try:
				return os.read(fd, BUF_LEN)
			finally:
				os.close(fd)
		except:
			pass

	def get_attr(self, name, attr, typ):
		buf = self._read(name, attr)
		if buf:
			return typ(buf)

	def get_profile(self, name):
		buf = self._read(name, ATTR_PROFILE)
		if buf:
			attr = json.loads(buf)
			return unicode2str(attr)
