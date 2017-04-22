#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from dev.driver import Driver
from lib.modes import MODE_OVP, MODE_SWITCH

class Trigger(Driver):
	def __init__(self, name=None):
		Driver.__init__(self, name=name, mode=MODE_OVP | MODE_SWITCH)

	def setup(self):
		self._cnt = 0

	def get(self):
		if self._cnt == 1:
			self._cnt += 1
			return {'enable':'true'}

	def open(self):
		self._cnt = 1
