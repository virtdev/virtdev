# local.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
import shutil
from lib.log import log_debug
from conf.virtdev import RSYNC
from conf.log import LOG_LOCALFS
from lib.util import DIR_MODE, call

class LocalFS(object):
	def _log(self, text):
		if LOG_LOCALFS:
			log_debug(self, text)

	def load(self, uid, src, dest):
		if RSYNC:
			call('rsync', '-a', src, dest)
		else:
			shutil.copyfile(src, dest)
		return True

	def save(self, uid, src, dest):
		if RSYNC:
			call('rsync', '-a', src, dest)
		else:
			shutil.copyfile(src, dest)
		return True

	def move(self, uid, src, dest):
		shutil.move(src, dest)

	def remove(self, uid, path):
		if os.path.isdir(path):
			shutil.rmtree(path)
		elif os.path.exists(path):
			os.remove(path)
		return True

	def mkdir(self, uid, path):
		os.makedirs(path, mode=DIR_MODE)
		return True

	def lsdir(self, uid, path):
		return os.listdir(path)

	def exist(self, uid, path):
		return os.path.exists(path)

	def touch(self, uid, path):
		open(path, 'a').close()
		return True

	def rename(self, uid, src, dest):
		os.rename(src, dest)
		return True

	def stat(self, uid, path):
		st = os.lstat(path)
		return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime', 'st_mode', 'st_mtime', 'st_nlink', 'st_size'))

	def truncate(self, uid, path, length):
		with open(path, 'r+') as f:
			f.truncate(length)
		return True

	def mtime(self, uid, path):
		return 0
