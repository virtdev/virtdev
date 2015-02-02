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

VDEV_REQ_GET   = 0x0001
VDEV_REQ_PUT   = 0x0002 
VDEV_REQ_OPEN  = 0x0004
VDEV_REQ_CLOSE = 0x0008
VDEV_REQ_PAIR  = 0x0010
VDEV_REQ_RESET = 0x0020

VDEV_REQ_SECRET = 'VIRTDEVLOGIN'

def parse(buf):
    if len(buf) < 8:
        return (None, None, None)
    index = struct.unpack('I', buf[0:4])[0]
    flags = struct.unpack('I', buf[4:8])[0]
    return (index, flags, buf[8:])

def _req_new(index, flags, buf=''):
    if index == None:
        index = 0
    return struct.pack('I', index) + struct.pack('I', flags) + buf

def req_open(index):
    return _req_new(index, VDEV_REQ_OPEN)

def req_close(index):
    return _req_new(index, VDEV_REQ_CLOSE)

def req_get(index):
    return _req_new(index, VDEV_REQ_GET)

def req_put(index, buf):
    return _req_new(index, VDEV_REQ_PUT, buf)

def req_pair():
    return _req_new(None, VDEV_REQ_PAIR, VDEV_REQ_SECRET)

def req_reset():
    return _req_new(None, VDEV_REQ_RESET)
