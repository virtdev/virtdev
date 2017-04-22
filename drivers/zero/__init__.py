#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from copy import copy
from dev.driver import Driver, wrapper

PRINT = False

class Zero(Driver):
	@wrapper
	def put(self, *args, **kwargs):
		output = copy(kwargs)
		if output and output.has_key('__cnt__'):
			if int(output['__cnt__']) == 0:
				if len(output) > 1:
					del output['__cnt__']
				if PRINT:
					print('Zero: output=%s' % str(output))
				return output
