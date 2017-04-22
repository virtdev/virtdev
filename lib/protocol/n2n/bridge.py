# bridge.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from lib.util import call
from threading import Thread
from conf.virtdev import BRIDGE_PORT

class Bridge(Thread):
	def run(self):
		call('supernode', '-l', str(BRIDGE_PORT))
