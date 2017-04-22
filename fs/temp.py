# temp.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
from lib.fields import TEMP, FIELDS
from lib.util import DIR_MODE, FILE_MODE, get_var_path

class Temp(object):
	def __init__(self, parent, rdonly):
		self._parent = parent
		self._rdonly = rdonly
		self._watcher = None
		if not self._rdonly:
			from watcher import Watcher
			self._watcher = Watcher()

	def _get_name(self, name):
		ret = ''
		length = len(name)
		for i in range(length):
			if name[i] == '/':
				ret += '_'
			else:
				ret += name[i]
		return ret

	def _get_dir(self, uid, field):
		path = get_var_path(uid)
		return os.path.join(path, TEMP, FIELDS[field])

	def _check_dir(self, uid, field):
		path = self._get_dir(uid, field)
		if not os.path.exists(path):
			os.makedirs(path, DIR_MODE)
		return path

	def _check_path(self, uid, name):
		name = self._get_name(name)
		dirname = self._check_dir(uid, self._parent.field)
		return self._get_path(dirname, name)

	def _get_path(self, dirname, name, suffix=''):
		path = os.path.join(dirname, name)
		if suffix:
			path += '.' + suffix
		return path

	def get_path(self, uid, name, suffix=''):
		name = self._get_name(name)
		dirname = self._get_dir(uid, self._parent.field)
		return self._get_path(dirname, name, suffix)

	def mtime(self, uid, name):
		path = self.get_path(uid, name, 'mtime')
		try:
			with open(path, 'r') as f:
				t = long(f.read().strip())
			return t
		except:
			return 0

	def set_mtime(self, uid, name, t):
		path = self.get_path(uid, name, 'mtime')
		with open(path, 'w') as f:
			f.write(str(t))

	def create(self, uid, name):
		path = self._check_path(uid, name)
		ret = os.open(path, os.O_RDWR | os.O_CREAT, FILE_MODE)
		if ret >= 0 and self._watcher:
			self._watcher.push(path)
		return ret

	def truncate(self, uid, name, length):
		path = self.get_path(uid, name)
		with open(path, 'r+') as f:
			f.truncate(length)

	def open(self, uid, name, flags):
		dest = self._check_path(uid, name)
		src = self._parent.get_path(uid, name)
		t = self._parent.get_mtime(uid, src)
		if not t or t != self.mtime(uid, name):
			self._parent.load(uid, src, dest)
			if t:
				self.set_mtime(uid, name, t)
		flg = os.O_RDWR
		if flags & os.O_TRUNC:
			flg |= os.O_TRUNC
		ret = os.open(dest, flg, FILE_MODE)
		if ret >= 0 and self._watcher:
			self._watcher.register(dest)
		return ret

	def release(self, uid, name, fh):
		os.close(fh)
		if not self._rdonly:
			path = self.get_path(uid, name)
			if self._watcher and self._watcher.pop(path):
				return True

	def commit(self, uid, name):
		src = self.get_path(uid, name)
		if os.path.exists(src):
			dest = self._parent.get_path(uid, name)
			self._parent.save(uid, src, dest)

	def discard(self, uid, name):
		path = self.get_path(uid, name)
		if os.path.exists(path):
			os.remove(path)
