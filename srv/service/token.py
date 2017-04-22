# token.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from service import Service

class Token(Service):
	def get(self, uid, name):
		device = self._query.device.get(name)
		if device:
			if uid != device['uid']:
				guests = self._query.guest.get(uid)
				if not guests or name not in guests:
					return
			return self._query.token.get(device['uid'])
