#      req.py
#      
#      Copyright (C) 2014 Yi-Wei Ci <ciyiwei@hotmail.com>
#      
#      This program is free software; you can redistribute it and/or modify
#      it under the terms of the GNU General Public License as published by
#      the Free Software Foundation; either version 2 of the License, or
#      (at your option) any later version.
#      
#      This program is distributed in the hope that it will be useful,
#      but WITHOUT ANY WARRANTY; without even the implied warranty of
#      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#      GNU General Public License for more details.
#      
#      You should have received a copy of the GNU General Public License
#      along with this program; if not, write to the Free Software
#      Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#      MA 02110-1301, USA.

import struct

REQ_GET   = 0x0001
REQ_PUT   = 0x0002 
REQ_OPEN  = 0x0004
REQ_CLOSE = 0x0008
REQ_MOUNT = 0x0010
REQ_SECRET = 'VIRTDEVLOGIN'

def parse(buf):
    if len(buf) < 8:
        return (None, None, None)
    index = struct.unpack('I', buf[0:4])[0]
    cmd = struct.unpack('I', buf[4:8])[0]
    return (index, cmd, buf[8:])

def _req(index, cmd, buf=''):
    if index == None:
        index = 0
    return struct.pack('I', index) + struct.pack('I', cmd) + buf

def req_get(index):
    return _req(index, REQ_GET)

def req_open(index):
    return _req(index, REQ_OPEN)

def req_close(index):
    return _req(index, REQ_CLOSE)

def req_put(index, buf):
    return _req(index, REQ_PUT, buf)

def req_mount():
    return _req(None, REQ_MOUNT, REQ_SECRET)
