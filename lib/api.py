# api.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
import xattr
from log import log
from operations import *
from conf.log import LOG_API
from util import get_mnt_path, get_cmd, call

def check_uid(func):
	def _check_uid(*args, **kwargs):
		uid = args[0]
		if 1 == len(args):
			path = get_mnt_path(uid)
		elif 2 == len(args):
			name = args[1]
			path = get_mnt_path(uid, name)
		else:
			raise Exception('Error: failed to invoke, invalid arguments')
		return func(path, **kwargs)
	return _check_uid

def _log(text):
	if LOG_API:
		log(text)

@check_uid
def api_enable(uid, **args):
	ret = xattr.setxattr(uid, OP_ENABLE, '')
	_log('api_enable: %s' % uid)
	return ret

@check_uid
def api_disable(uid, **args):
	ret = xattr.setxattr(uid, OP_DISABLE, '')
	_log('api_disable: %s' % uid)
	return ret

@check_uid
def api_mount(uid, **attr):
	xattr.setxattr(uid, OP_MOUNT, str(attr))
	_log('api_mount: %s' % uid)

@check_uid
def api_invalidate(uid):
	xattr.setxattr(uid, OP_INVALIDATE, '', symlink=True)
	_log('api_invalidate: %s' % uid)

@check_uid
def api_clone(uid, **attr):
	ret = xattr.getxattr(uid, get_cmd(OP_CLONE, attr))
	_log('api_clone: %s' % uid)
	return ret

@check_uid
def api_combine(uid, **attr):
	ret = xattr.getxattr(uid, get_cmd(OP_COMBINE, attr))
	_log('api_combine: %s' % uid)
	return ret

@check_uid
def api_create(uid, **attr):
	ret = xattr.getxattr(uid, get_cmd(OP_CREATE, attr))
	_log('api_create: %s' % uid)
	return ret

@check_uid
def api_poll(uid):
	ret = xattr.getxattr(uid, OP_POLL)
	_log('api_poll: %s' % uid)
	return ret

@check_uid
def api_scan(uid):
	ret = xattr.getxattr(uid, OP_SCAN)
	_log('api_scan: %s' % uid)
	return ret

@check_uid
def api_exist(uid):
	os.path.exists(uid)
	_log('api_exist: %s' % uid)

@check_uid
def api_touch(uid):
	call('touch', uid)
	_log('api_touch: %s' % uid)
