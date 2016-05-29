#      io.py
#      
#      Copyright (C) 2016 Yi-Wei Ci <ciyiwei@hotmail.com>
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
import socket
import struct

DLE = '@'
STX = '0'
ETX = '1'
CHR = DLE + DLE
HEAD = DLE + STX
TAIL = DLE + ETX
SIZE = 1 << 26

def _check(buf):
    if len(buf) < crc.CRC_SIZE:
        return
    tmp = buf[crc.CRC_SIZE:]
    if crc.encode(tmp) == struct.unpack('H', buf[0:crc.CRC_SIZE])[0]:
        return tmp

def send_pkt(sock, buf):
    head = struct.pack('I', len(buf))
    sock.sendall(head)
    if buf:
        sock.sendall(buf)

def recv_bytes(sock, length):
    ret = []
    while length > 0:
        buf = sock.recv(min(length, 2048))
        if not buf:
            raise Exception('Error: failed to receive bytes')
        ret.append(buf)
        length -= len(buf) 
    return ''.join(ret)

def recv_pkt(sock):
    head = recv_bytes(sock, 4)
    if not head:
        return ''
    length = struct.unpack('I', head)[0]
    return recv_bytes(sock, length)

def put(sock, buf, local=False):
    buf = str(buf)
    if local:
        send_pkt(sock, buf)
        return
    code = crc.encode(buf)
    tmp = str(struct.pack('H', code) + buf).split(DLE)
    out = HEAD + tmp[0]
    for i in range(1, len(tmp)):
        out += CHR + tmp[i]
    out += TAIL
    sock.send(out)

def get(sock, local=False):
    if local:
        return recv_pkt(sock)
    buf = ''
    start = False
    while True:
        ch = sock.recv(1)
        if ch == DLE:
            ch = sock.recv(1)
            if ch == DLE:
                if start:
                    if len(buf) < SIZE:
                        buf += DLE
                    else:
                        raise Exception('Error: failed to get, invalid length')
            elif ch == STX:
                start = True
                buf = ''
            elif ch == ETX:
                if start:
                    out = _check(buf)
                    if out:
                        return out
                    else:
                        start = False
                        buf = ''
        elif start:
            if len(buf) < SIZE:
                buf += ch
            else:
                raise Exception('Error: failed to get, invalid length')

def connect(addr, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((addr, port))
    return sock

def close(sock):
    if sock:
        sock.close()
