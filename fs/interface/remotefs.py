# remotefs.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from lib.log import log_debug
from conf.log import LOG_REMOTEFS
from conf.prot import PROT_REMOTEFS

if PROT_REMOTEFS == 'hadoop':
	from module.hadoop import Hadoop as FS

class RemoteFS(FS):
	def _log(self, text):
		if LOG_REMOTEFS:
			log_debug(self, text)
