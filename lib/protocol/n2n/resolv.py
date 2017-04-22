# resolv.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from random import randint

F1_SIZE = 255 * 8
F2_SIZE = 8
F3_SIZE = 32
HOST_MAX = 255 * 255 * 8

class Resolv(object):
	def _gen_addr(self):
		index = randint(0, HOST_MAX - 1)
		tmp = index % F1_SIZE
		f1 = int(index / F1_SIZE)
		f2 = int(tmp / F2_SIZE)
		f3 = (tmp % F2_SIZE) * F3_SIZE + 1
		return '10.%d.%d.%d' % (f1, f2, f3)

	def get_addr(self, uid, node):
		return self._gen_addr()
