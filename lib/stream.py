#      stream.py
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

import crc
import struct
from util import send_pkt, recv_pkt

STREAM_DLE = '@'
STREAM_STX = '0'
STREAM_ETX = '1'
STREAM_CHR = STREAM_DLE + STREAM_DLE
STREAM_HEAD = STREAM_DLE + STREAM_STX
STREAM_TAIL = STREAM_DLE + STREAM_ETX
STREAM_MAX_LEN = 1 << 22

def _check(buf):
    if len(buf) < crc.CRC_SIZE:
        return
    tmp = buf[crc.CRC_SIZE:]
    if crc.encode(tmp) == struct.unpack('H', buf[0:crc.CRC_SIZE])[0]:
        return tmp

def put(sock, buf, anon=False):
    if anon:
        send_pkt(sock, buf)
        return
    code = crc.encode(buf)
    tmp = str(struct.pack('H', code) + buf).split(STREAM_DLE)
    out = STREAM_HEAD + tmp[0]
    for i in range(1, len(tmp)):
        out += STREAM_CHR + tmp[i]
    out += STREAM_TAIL
    sock.send(out)

def get(sock, anon=False):
    if anon:
        return recv_pkt(sock)
    buf = ''
    start = False
    while True:
        ch = sock.recv(1)
        if ch == STREAM_DLE:
            ch = sock.recv(1)
            if ch == STREAM_DLE:
                if start:
                    if len(buf) < STREAM_MAX_LEN:
                        buf += STREAM_DLE
                    else:
                        raise Exception('Invalid length of stream')
            elif ch == STREAM_STX:
                start = True
                buf = ''
            elif ch == STREAM_ETX:
                if start:
                    out = _check(buf)
                    if out:
                        return out
                    else:
                        start = False
                        buf = ''
        elif start:
            if len(buf) < STREAM_MAX_LEN:
                buf += ch
            else:
                raise Exception('Invalid length of stream')
