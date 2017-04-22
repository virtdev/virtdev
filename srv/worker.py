# worker.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from lib import io
from lib import bson
from lib import codec
from service.key import Key
from threading import Thread
from service.user import User
from service.node import Node
from conf.log import LOG_WORKER
from service.token import Token
from service.guest import Guest
from service.device import Device
from conf.meta import WORKER_PORT
from lib.log import log_debug, log_err
from multiprocessing import TimeoutError
from multiprocessing.pool import ThreadPool
from SocketServer import BaseRequestHandler
from lib.util import UID_SIZE, unicode2str, create_server

TIMEOUT = 120 # seconds
_services = {}

class WorkerServer(BaseRequestHandler):
	def handle(self):
		try:
			pkt = io.recv_pkt(self.request)
			if pkt:
				reqest = unicode2str(bson.loads(pkt))
				uid = reqest['uid']
				token = reqest['token']
				buf = reqest['buf']
				req = codec.decode(buf, token)
				if not req:
					log_err(self, 'failed to handle, invalid request')
					return
			else:
				log_err(self, 'failed to handle')
				return
			op = req.get('op')
			srv = req.get('srv')
			args = req.get('args')
			if not op or not srv:
				log_err(self, 'failed to handle, invalid arguments')
				return
			args.update({'uid':uid})
			if not _services.has_key(srv):
				log_err(self, 'failed to handle, invalid service %s' % str(srv))
				return
			pool = ThreadPool(processes=1)
			result = pool.apply_async(_services[srv].proc, args=(op, args))
			pool.close()
			res = ''
			try:
				res = result.get(timeout=TIMEOUT)
			except TimeoutError:
				log_err(self, 'failed to handle, timeout')
			finally:
				pool.join()
			res = codec.encode(res, token, buf[:UID_SIZE])
			io.send_pkt(self.request, bson.dumps({'':res}))
		except:
			pass

class Worker(object):
	def __init__(self, addr, query):
		self._init_services(query)
		self._addr = addr

	def _log(self, text):
		if LOG_WORKER:
			log_debug(self, text)

	def _add_service(self, srv):
		name = str(srv)
		if name not in _services:
			_services.update({str(srv):srv})

	def _init_services(self, query):
		self._add_service(Key(query))
		self._add_service(User(query))
		self._add_service(Node(query))
		self._add_service(Guest(query))
		self._add_service(Token(query))
		self._add_service(Device(query))

	def start(self):
		self._log('start ...')
		Thread(target=create_server, args=(self._addr, WORKER_PORT, WorkerServer)).start()
