# log.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from datetime import datetime
from conf.defaults import SHOW_ERROR, SHOW_DEBUG, SHOW_WARNNING

def _get_name(obj):
	if type(obj) != str:
		return obj.__class__.__name__
	else:
		return obj

def log_get(obj, text):
	return _get_name(obj) + ': ' + str(text)

def log(text, time=False, force=False):
	if SHOW_DEBUG or force:
		if time:
			print(str(text) + '  %s' % str(datetime.utcnow()))
		else:
			print(str(text))

def log_err(obj, text, time=True):
	if SHOW_ERROR:
		if obj:
			text = log_get(obj, text)
		else:
			text = log_get('Error', text)
		log(text, time=time, force=True)

def log_warnning(obj, text, time=False):
	if SHOW_WARNNING:
		if obj:
			text = log_get(obj, text)
		else:
			text = log_get('Warnning', text)
		log(text, time=time)

def log_debug(obj, text, time=False):
	if SHOW_DEBUG:
		if obj:
			text = log_get(obj, text)
		else:
			text = log_get('Debug', text)
		log(text, time=time)
