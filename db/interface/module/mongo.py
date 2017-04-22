# mongo.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import pymongo
from pymongo import MongoClient
from conf.meta import META_SERVER_PORT
from pymongo.collection import Collection

DATABASE = 'test'

class Mongo(object):
	def __init__(self, name):
		self._name = name

	def connect(self, addr):
		db = pymongo.database.Database(MongoClient(addr, META_SERVER_PORT), DATABASE)
		return Collection(db, self._name)

	def get(self, conn, key):
		return conn.find_one(key)

	def put(self, conn, key, val, create=False):
		conn.update(key, val, upsert=create)

	def delete(self, conn, key):
		conn.remove(key)

	def connection(self, coll):
		return coll
