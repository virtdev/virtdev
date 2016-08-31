# api.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
import xattr
from operations import *
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

@check_uid
def enable(uid, **args):
    return xattr.setxattr(uid, OP_ENABLE, '')

@check_uid
def disable(uid, **args):
    return xattr.setxattr(uid, OP_DISABLE, '')

@check_uid
def mount(uid, **attr):
    xattr.setxattr(uid, OP_MOUNT, str(attr))

@check_uid
def invalidate(uid):
    xattr.setxattr(uid, OP_INVALIDATE, '', symlink=True)

@check_uid
def clone(uid, **attr):
    return xattr.getxattr(uid, get_cmd(OP_CLONE, attr))

@check_uid
def combine(uid, **attr):
    return xattr.getxattr(uid, get_cmd(OP_COMBINE, attr))

@check_uid
def create(uid, **attr):
    return xattr.getxattr(uid, get_cmd(OP_CREATE, attr))

@check_uid
def poll(uid):
    return xattr.getxattr(uid, OP_POLL)

@check_uid
def scan(uid):
    return xattr.getxattr(uid, OP_SCAN)

@check_uid
def exists(uid):
    os.path.exists(uid)

@check_uid
def touch(uid):
    call('touch', uid)
