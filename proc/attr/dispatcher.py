# dispatcher.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from proc import proc
from lib.pool import Pool
from random import randint
from lib.queue import Queue
from lib.loader import Loader
from lib.lock import NamedLock
from conf.defaults import DEBUG
from threading import Lock, Thread
from conf.log import LOG_DISPATCHER
from lib.attributes import ATTR_DISPATCHER
from lib.log import log_debug, log_err, log_get
from conf.defaults import PROC_ADDR, DISPATCHER_PORT
from lib.util import lock, named_lock, edge_lock, is_local

ASYNC = True
POOL_SIZE = 2
QUEUE_LEN = 16

class DispatcherQueue(Queue):
	def __init__(self, scheduler):
		Queue.__init__(self, QUEUE_LEN)
		self._scheduler = scheduler

	def proc(self, buf):
		self._scheduler.proc(self.index, *buf)

class DispatcherScheduler(object):
	def __init__(self, core):
		self._dest = {}
		self._busy = {}
		self._core = core
		self._lock = Lock()
		self._pool = Pool()
		for _ in range(POOL_SIZE):
			self._pool.add(DispatcherQueue(self))

	@lock
	def select(self, dest, src, buf, flags):
		if self._busy.has_key(src):
			return False

		pos = None
		length = None
		for i in range(POOL_SIZE):
			if src != self._dest.get(i):
				queue = self._pool.get(i)
				l = queue.length
				if l < QUEUE_LEN:
					if pos == None or length > l:
						length = l
						pos = i
			else:
				return False

		if pos != None:
			queue = self._pool.get(pos)
			if not queue.push((dest, src, buf, flags)):
				log_err(self, 'failed to select')
				raise Exception(log_get(self, 'failed to select'))
			return True

	@lock
	def _acquire_queue(self, index, dest):
		self._dest.update({index:dest})

	@lock
	def _release_queue(self, index):
		del self._dest[index]

	def _proc(self, index, dest, src, buf, flags):
		self._acquire_queue(index, dest)
		try:
			self._core.put(dest, src, buf, flags)
		finally:
			self._release_queue(index)

	def _proc_safe(self, index, dest, src, buf, flags):
		try:
			self._proc(index, dest, src, buf, flags)
		except:
			log_err(self, 'failed to process')

	def proc(self, index, dest, src, buf, flags):
		if DEBUG:
			self._proc(index, dest, src, buf, flags)
		else:
			self._proc_safe(index, dest, src, buf, flags)

	@lock
	def _acquire(self, dest):
		if self._busy.has_key(dest):
			self._busy[dest] += 1
		else:
			self._busy[dest] = 1

	@lock
	def _release(self, dest):
		if self._busy.has_key(dest):
			self._busy[dest] -= 1
			if self._busy[dest] <= 0:
				del self._busy[dest]

	def put(self, dest, src, buf, flags):
		self._acquire(dest)
		try:
			self._core.put(dest, src, buf, flags)
		finally:
			self._release(dest)

	def wait(self):
		self._pool.wait()

class Dispatcher(object):
	def __init__(self, uid, channel, core, addr=PROC_ADDR):
		self._uid = uid
		self._queue = []
		self._paths = {}
		self._hidden = {}
		self._core = core
		self._addr = addr
		self._visible = {}
		self._scheduler = None
		self._dispatchers = {}
		self._channel = channel
		self._lock = NamedLock()
		self._loader = Loader(self._uid)
		if ASYNC and QUEUE_LEN:
			self._scheduler = DispatcherScheduler(core)

	def _log(self, text):
		if LOG_DISPATCHER:
			log_debug(self, text)

	def _get_code(self, name):
		buf = self._dispatchers.get(name)
		if not buf:
			buf = self._loader.get_attr(name, ATTR_DISPATCHER, str)
			self._dispatchers.update({name:buf})
		return buf

	def _deliver(self, dest, src, buf, flags):
		try:
			if ASYNC:
				if QUEUE_LEN:
					while True:
						ret = self._scheduler.select(dest, src, buf, flags)
						if ret == None:
							self._scheduler.wait()
						else:
							if not ret:
								Thread(target=self._scheduler.put, args=(dest, src, buf, flags)).start()
							break
				else:
					Thread(target=self._core.put, args=(dest, src, buf, flags)).start()
			else:
				self._core.put(dest, src, buf, flags)
		except:
			log_err(self, 'failed to deliver, dest=%s, src=%s' % (dest, src))

	def _send(self, dest, src, buf, flags):
		self._channel.put(dest, src, buf=buf, flags=flags)

	@edge_lock
	def add_edge(self, edge, hidden=False):
		src = edge[0]
		dest = edge[1]

		if hidden:
			paths = self._hidden
		else:
			paths = self._visible

		if paths.has_key(src) and paths[src].has_key(dest):
			return

		if not paths.has_key(src):
			paths[src] = {}

		if not self._paths.has_key(src):
			self._paths[src] = {}

		local = is_local(self._uid, dest)
		paths[src].update({dest:local})
		if not self._paths[src].has_key(dest):
			if not local:
				self._channel.allocate(dest)
			self._paths[src].update({dest:1})
		else:
			self._paths[src][dest] += 1
		self._log('add edge, dest=%s, src=%s, local=%s' % (dest, src, str(local)))

	@edge_lock
	def remove_edge(self, edge, hidden=False):
		src = edge[0]
		dest = edge[1]
		if hidden:
			paths = self._hidden
		else:
			paths = self._visible
		if not paths.has_key(src) or not paths[src].has_key(dest):
			return
		local = paths[src][dest]
		del paths[src][dest]
		self._paths[src][dest] -= 1
		if 0 == self._paths[src][dest]:
			del self._paths[src][dest]
			if not local:
				self._channel.free(dest)
		self._log('remove edge, dest=%s, src=%s, local=%s' % (dest, src, str(local)))

	@named_lock
	def has_edge(self, name):
		return self._visible.has_key(name)

	def update_edges(self, name, edges):
		if not edges:
			return
		for dest in edges:
			if dest.startswith('.'):
				dest = dest[1:]
			if dest != name:
				self.add_edge((name, dest))

	def remove_edges(self, name):
		paths = self._visible.get(name)
		for i in paths:
			self.remove_edge((name, i))
		paths = self._hidden.get(name)
		for i in paths:
			self.remove_edge((name, i), hidden=True)
		if self._dispatchers.has_key(name):
			del self._dispatchers[name]

	def remove(self, name):
		if self._dispatchers.has_key(name):
			del self._dispatchers[name]

	def sendto(self, dest, src, buf, hidden=False, flags=0):
		if not buf:
			return
		self.add_edge((src, dest), hidden=hidden)
		if self._hidden:
			local = self._hidden[src][dest]
		else:
			local = self._visible[src][dest]
		if not local:
			self._send(dest, src, buf, flags)
		else:
			self._deliver(dest, src, buf, flags)
		self._log('sendto, dest=%s, src=%s' % (dest, src))

	def send(self, name, buf, flags=0):
		if not buf:
			return
		dest = self._visible.get(name)
		if not dest:
			return
		for i in dest:
			if not dest[i]:
				self._send(i, name, buf, flags)
			else:
				self._deliver(i, name, buf, flags)
			self._log('send, dest=%s, src=%s' % (i, name))

	def send_blocks(self, name, blocks):
		if not blocks:
			return
		dest = self._visible.get(name)
		if not dest:
			return
		cnt = 0
		keys = dest.keys()
		len_keys = len(keys)
		len_blks = len(blocks)
		window = (len_blks + len_keys - 1) / len_keys
		start = randint(0, len_keys - 1)
		for _ in range(len_keys):
			i = keys[start]
			for _ in range(window):
				if blocks[cnt]:
					if not dest[i]:
						self._send(i, name, blocks[cnt], 0)
					else:
						self._deliver(i, name, blocks[cnt], 0)
					self._log('send a block, dest=%s, src=%s' % (i, name))
				cnt += 1
				if cnt == len_blks:
					return
			start += 1
			if start == len_keys:
				start = 0

	def put(self, name, buf):
		try:
			code = self._get_code(name)
			if code == None:
				code = self._get_code(name)
				if not code:
					return
			return proc.put(self._addr, DISPATCHER_PORT, code, buf)
		except:
			log_err(self, 'failed to put')

	def check(self, name):
		if self._dispatchers.get(name):
			return True
		else:
			buf = self._loader.get_attr(name, ATTR_DISPATCHER, str)
			if buf:
				self._dispatchers.update({name:buf})
				return True
