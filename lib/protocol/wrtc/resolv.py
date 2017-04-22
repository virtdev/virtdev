# resolv.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import uuid

class Resolv(object):
	def get_addr(self, uid, node):
		return uuid.uuid4().hex
