# sophiadb.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from sophia import Database

class SophiaDB(object):
	def __init__(self, name):
		self._db = Database()
		self._db.open(name)

	def bench(self):
		self._db.begin()

	def commit(self, bench=None):
		self._db.commit()

	def keys(self):
		return list(self._db.iterkeys())

	def get(self, key):
		return self._db.get(key)

	def put(self, key, val):
		return self._db.put(key, val)
