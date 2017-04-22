# database.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from cache import Cache
from lib.log import log_err, log_get
from lib.domains import DOMAIN_DEV, DOMAIN_USR
from interface.commondb import CommonDB, VAL_NAME

class Database(object):
	def __init__(self, multi_value=False, cache_port=None, router=None, domain=None):
		self._db = CommonDB(name=self.name, router=router, domain=domain)
		self._multi_value = multi_value
		if cache_port != None:
			self._cache = Cache(cache_port)
		else:
			self._cache = None

	@property
	def name(self):
		return self.__class__.__name__.lower()

	def get(self, key, first=False):
		val = None
		cache = False
		if self._cache:
			cache = True
			if not self._multi_value or first:
				val = self._cache.get(key)

		if not val:
			coll = self._db.collection(key)
			conn = self._db.connection(coll)
			val = self._db.get(conn, key)

			if self._multi_value:
				if first and type(val) == list:
					val = val[0]
				else:
					cache = False

			if cache:
				self._cache.put(key, val)
		return val

	def put(self, key, value):
		if self._cache:
			self._cache.delete(key)
		if not self._multi_value:
			val = {'$set':{VAL_NAME:value}}
		else:
			val = {'$addToSet':{VAL_NAME:value}}
		coll = self._db.collection(key)
		conn = self._db.connection(coll)
		self._db.put(conn, key, val, create=True)

	def delete(self, key, value=None, regex=False):
		if self._cache:
			self._cache.delete(key)

		coll = self._db.collection(key)
		conn = self._db.connection(coll)
		if not value:
			self._db.delete(conn, key)
		else:
			if not self._multi_value:
				log_err(self, 'failed to delete')
				raise Exception(log_get(self, 'failed to delete'))
			if not regex:
				self._db.put(conn, key, {'$pull':{VAL_NAME:value}})
			else:
				self._db.put(conn, key, {'$pull':{VAL_NAME:{'$regex':value}}})

class Token(Database):
	def __init__(self, router):
		Database.__init__(self, multi_value=True, router=router, domain=DOMAIN_USR)

class Guest(Database):
	def __init__(self, router):
		Database.__init__(self, multi_value=True, router=router, domain=DOMAIN_USR)

class Node(Database):
	def __init__(self, router):
		Database.__init__(self, multi_value=True, router=router, domain=DOMAIN_USR)

class Member(Database):
	def __init__(self, router):
		Database.__init__(self, multi_value=True, router=router, domain=DOMAIN_USR)

class Device(Database):
	def __init__(self, router):
		Database.__init__(self, router=router, domain=DOMAIN_DEV)

class Key(Database):
	def __init__(self, router):
		Database.__init__(self, router=router, domain=DOMAIN_DEV)
