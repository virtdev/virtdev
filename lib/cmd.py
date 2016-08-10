# cmd.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import struct

CMD_GET   = 0x0001
CMD_PUT   = 0x0002 
CMD_OPEN  = 0x0004
CMD_CLOSE = 0x0008
CMD_MOUNT = 0x0010

SECRET = 'VIRTDEVLOGIN'

def parse(buf):
    if len(buf) < 8:
        return (None, None, None)
    index = struct.unpack('I', buf[0:4])[0]
    cmd = struct.unpack('I', buf[4:8])[0]
    return (index, cmd, buf[8:])

def _get_cmd(index, cmd, buf=''):
    if index == None:
        index = 0
    return struct.pack('I', index) + struct.pack('I', cmd) + buf

def cmd_get(index):
    return _get_cmd(index, CMD_GET)

def cmd_open(index):
    return _get_cmd(index, CMD_OPEN)

def cmd_close(index):
    return _get_cmd(index, CMD_CLOSE)

def cmd_put(index, val):
    return _get_cmd(index, CMD_PUT, val)

def cmd_mount():
    return _get_cmd(None, CMD_MOUNT, SECRET)
