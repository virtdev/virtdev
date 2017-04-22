# edge.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
from entry import Entry
from  errno import EINVAL
from lib.log import log_err
from fuse import FuseOSError

class Edge(Entry):
	def can_touch(self):
		return True

	def can_unlink(self):
		return True

	def initialize(self, uid, edge, hidden=False):
		if type(edge) != tuple:
			log_err(self, 'failed to initialize')
			raise FuseOSError(EINVAL)

		if not hidden:
			name = os.path.join(edge[0], edge[1])
		else:
			name = os.path.join(edge[0], '.' + edge[1])
		self.create(uid, name)

	def _add_edge(self, src, dest):
		if self._core:
			if dest.startswith('.'):
				edge = (src, dest[1:])
			else:
				edge = (src, dest)
			self._core.add_edge(edge)

	def _create(self, uid, name):
		if self._core:
			parent = self.parent(name)
			child = self.child(name)
			if parent != child:
				self._add_edge(parent, child)

	def create(self, uid, name):
		self.symlink(uid, name)
		self._create(uid, name)
		return 0

	def open(self, uid, name, flags):
		return self.create(uid, name)

	def _unlink(self, uid, name):
		if not self._core:
			return
		parent = self.parent(name)
		child = self.child(name)
		if parent != child:
			edge = (parent, child)
			self._core.remove_edge(edge)

	def unlink(self, uid, name):
		self._unlink(uid, name)
		self.remove(uid, name)

	def readdir(self, uid, name):
		return self.lsdir(uid, name)

	def readlink(self, uid, name):
		return self.lslink(uid, name)

	def getattr(self, uid, name):
		return self.lsattr(uid, name, symlink=True)
