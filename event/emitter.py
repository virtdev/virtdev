# emitter.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import zerorpc
from lib.domains import *
from lib.log import log_err
from lib.util import zmqaddr
from conf.meta import EVENT_COLLECTOR_PORT

class EventEmitter(object):
	def __init__(self, router):
		self._router = router

	def _put(self, addr, uid, name):
		cli = zerorpc.Client()
		cli.connect(zmqaddr(addr, EVENT_COLLECTOR_PORT))
		try:
			cli.put(uid, name)
		finally:
			cli.close()

	def put(self, uid, name):
		try:
			addr = self._router.get(uid, DOMAIN_USR)
			if addr:
				self._put(addr, uid, name)
		except:
			log_err(self, 'failed to put')
