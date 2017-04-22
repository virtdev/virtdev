# queue.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from conf.log import LOG_QUEUE
from conf.defaults import DEBUG
from log import log_debug, log_err
from threading import Thread, Event, Lock
from multiprocessing.pool import ThreadPool

TIMEOUT = 3600 # seconds
WAIT_TIME = 0.1 # seconds

class Queue(object):
	def __init__(self, capacity, timeout=TIMEOUT):
		self.__thread = Thread(target=self.__run)
		self.__capacity = capacity
		self.__timeout = timeout
		self.__event = Event()
		self.__lock = Lock()
		self.__parent = None
		self.__index = None
		self.__queue = []
		self.__thread.start()

	def __log(self, text):
		if LOG_QUEUE:
			log_debug(self, text)

	def set_index(self, index):
		self.__index = index

	def set_parent(self, parent):
		self.__parent = parent

	def get_parent(self):
		return self.__parent

	def proc(self, buf):
		pass

	def insert(self, buf):
		self.__lock.acquire()
		try:
			self.__queue.insert(0, buf)
			self.__event.set()
			return True
		finally:
			self.__lock.release()

	def push(self, buf):
		self.__lock.acquire()
		try:
			if len(self.__queue) < self.__capacity:
				self.__queue.append(buf)
				self.__event.set()
				return True
		finally:
			self.__lock.release()

	@property
	def index(self):
		return self.__index

	@property
	def length(self):
		return len(self.__queue)

	@property
	def capacity(self):
		return self.__capacity

	def __pop(self):
		self.__lock.acquire()
		try:
			if len(self.__queue) > 0:
				buf = self.__queue.pop(0)
				if len(self.__queue) == 0:
					self.__event.clear()
				if buf:
					if self.__parent:
						self.__parent.wakeup()
					self.__log('index=%d, length=%d' % (self.index, self.length))
					return buf
		finally:
			self.__lock.release()

	def __proc(self, buf):
		pool = ThreadPool(processes=1)
		result = pool.apply_async(self.proc, args=(buf,))
		pool.close()
		try:
			result.get(timeout=self.__timeout)
		finally:
			pool.join()

	def __proc_safe(self, buf):
		try:
			self.__proc(buf)
		except:
			log_err(self, 'failed to process')

	def __wait(self):
		self.__event.wait(WAIT_TIME)

	def __run(self):
		while True:
			self.__wait()
			buf = self.__pop()
			if buf:
				if DEBUG:
					self.__proc(buf)
				else:
					self.__proc_safe(buf)
