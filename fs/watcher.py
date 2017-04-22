# watcher.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import sys
import select
import signal
import inotify
from errno import EINTR
from inotify.watcher import AutoWatcher
from threading import Thread, Event, Lock

_results = {}
_thread = None
_lock = Lock()
_event = Event()
_watcher = AutoWatcher()

def _watch():
	while True:
		try:
			if 0 == _watcher.num_watches():
				_event.wait()
				_event.clear()
			for e in _watcher.read():
				path = e.fullpath
				if _watcher.path(path):
					_results[path] = True
					_watcher.remove_path(path)
		except(OSError, select.error) as EINTR:
			continue

def _term(signal, frame):
	_watcher.close()
	sys.exit(0)

signal.signal(signal.SIGTERM, _term)

class Watcher(object):
	def __init__(self):
		_lock.acquire()
		try:
			global _thread
			if not _thread:
				_thread = Thread(target=_watch)
				_thread.start()
		finally:
			_lock.release()

	def register(self, path):
		n = _watcher.num_watches()
		_watcher.add(path, inotify.IN_MODIFY)
		if n == 0:
			_event.set()

	def push(self, path):
		_results[path] = True

	def pop(self, path):
		try:
			return _results.pop(path)
		except:
			pass
