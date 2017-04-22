# link.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import sig
from operations import *
from modes import MODE_LINK, MODE_CLONE
from util import str2tuple, update_device
from log import log_debug, log_err, log_get

def chkop(func):
	def _chkop(self, name, op, **args):
		if op not in self.operations:
			raise Exception('Error: invalid operation, op=%s' % str(op))
		return func(self, name, op, **args)
	return _chkop

class Uplink(object):
	def __init__(self, manager):
		self.operations = [OP_ADD, OP_GET]
		self._manager = manager

	@chkop
	def put(self, name, op, **args):
		if op == OP_GET:
			return self._manager.device.get(name, **args)
		elif op == OP_ADD:
			return self._manager.device.add(name, **args)

class Downlink(object):
	def __init__(self, query):
		self.operations = [OP_MOUNT, OP_INVALIDATE, OP_TOUCH, OP_ENABLE, OP_DISABLE, OP_JOIN, OP_ACCEPT]
		self._query = query

	def _get_device(self, name):
		return self._query.device.get(name)

	def _check(self, uid, node, addr):
		try:
			token = self._query.token.get(uid)
			if not token:
				log_err(self, 'failed to check, no token')
				return
			if sig.exist(uid, addr, token):
				return token
		except:
			log_debug(self, 'failed to check, addr=%s' % str(addr))

	def _request(self, uid, addr, op, args, token):
		try:
			req = {'op':op, 'args':args}
			sig.send(uid, addr, req, token)
			return True
		except:
			log_err(self, 'failed to request, addr=%s, op=%s' % (addr, op))

	def mount(self, uid, name, mode, vrtx, typ, parent, timeout):
		node = None
		addr = None
		token = False

		if vrtx and not parent:
			parent = vrtx[0]

		if parent:
			res = self._query.device.get(parent)
			if not res or res.get('uid') != uid:
				log_err(self, 'failed to mount, invalid uid')
				raise Exception(log_get(self, 'failed to mount'))

			addr = res.get('addr')
			if not addr:
				log_err(self, 'failed to mount, invalid addr')
				raise Exception(log_get(self, 'failed to mount'))

			members = self._query.member.get(uid)
			if not members:
				log_err(self, 'failed to mount, cannot get members')
				raise Exception(log_get(self, 'failed to mount'))

			for i in members:
				p, node = str2tuple(i)
				if p == parent:
					token = self._check(uid, node, addr)
					if not token:
						log_err(self, 'failed to mount')
						raise Exception(log_get(self, 'failed to mount'))
					break
		else:
			nodes = self._query.node.get(uid)
			if not nodes:
				log_err(self, 'failed to mount, cannot get nodes')
				raise Exception(log_get(self, 'failed to mount'))

			for i in nodes:
				node, addr, _ = str2tuple(i)
				token = self._check(uid, node, addr)
				if token:
					break

		if not token:
			log_err(self, 'failed to mount')
			raise Exception(log_get(self, 'failed to mount'))

		attr = {}
		attr.update({'type':typ})
		attr.update({'name':name})
		attr.update({'vertex':vrtx})
		attr.update({'mode':mode | MODE_LINK})
		if mode & MODE_CLONE:
			attr.update({'parent':parent})
		if vrtx:
			attr.update({'timeout':timeout})

		args = {'attr':str(attr)}
		ret = self._request(uid, addr, OP_MOUNT, args, token)
		update_device(self._query, uid, node, addr, name)
		return ret

	@chkop
	def put(self, name, op, **args):
		if name:
			res = self._get_device(name)
			uid = res['uid']
			addr = res['addr']
			node = res['node']
		else:
			uid = args.pop('uid')
			addr = args.pop('addr')
			node = args.pop('node')

		token = self._check(uid, node, addr)
		if token:
			return self._request(uid, addr, op, args, token)
