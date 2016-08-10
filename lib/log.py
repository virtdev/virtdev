# log.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from datetime import datetime
from conf.log import LOG_DEBUG, LOG_ERR, LOG_WARNNING

def _get_name(obj):
    if type(obj) != str:
        return obj.__class__.__name__
    else:
        return obj

def log_get(obj, text):
    return _get_name(obj) + ': ' + str(text)

def log(text, time=False, force=False):
    if LOG_DEBUG or force:
        if time:
            print(str(text) + '  %s' % str(datetime.utcnow()))
        else:
            print(str(text))

def log_err(obj, text, time=True):
    if LOG_ERR:
        if obj:
            text = log_get(obj, text)
        else:
            text = log_get('Error', text)
        log(text, time=time, force=True)

def log_warnning(obj, text, time=False):
    if LOG_WARNNING:
        if obj:
            text = log_get(obj, text)
        else:
            text = log_get('Warnning', text)
        log(text, time=time)

def log_debug(obj, text, time=False):
    if LOG_DEBUG:
        if obj:
            text = log_get(obj, text)
        else:
            text = log_get('Debug', text)
        log(text, time=time)
