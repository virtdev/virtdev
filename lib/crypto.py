#      crypto.py
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

import json
import struct
from util import UID_SIZE
from Crypto.Cipher import AES
from lib.log import log_err, log_get
from binascii import b2a_hex, a2b_hex

HEAD_SIZE = len(struct.pack('I', 0))

class VDevCrypto():
    def __init__(self, key):
        self._key = key[0:16]
        self._iv = key[16:32]
    
    def encrypt(self, text):
        crypto = AES.new(self._key, AES.MODE_CBC, self._iv)
        buf = struct.pack('I', len(text)) + text
        buf += (16 - len(buf) % 16) * '\0'
        return b2a_hex(crypto.encrypt(buf))
    
    def decrypt(self, text):
        crypto = AES.new(self._key, AES.MODE_CBC, self._iv)
        buf = crypto.decrypt(a2b_hex(text))
        if len(buf) < HEAD_SIZE:
            log_err(self, 'failed to decrypt')
            raise Exception(log_get(self, 'failed to decrypt'))
        length = struct.unpack('I', buf[0:HEAD_SIZE])[0]
        if len(buf) < HEAD_SIZE + length:
            log_err(self, 'failed to decrypt')
            raise Exception(log_get(self, 'failed to decrypt'))
        return buf[HEAD_SIZE:HEAD_SIZE + length]

def pack(uid, buf, token):
    crypto = VDevCrypto(token)
    return uid + crypto.encrypt(json.dumps(buf))

def unpack(uid, buf, token):
    length = len(buf)
    if length <= UID_SIZE or (uid and buf[0:UID_SIZE] != uid):
        return
    crypto = VDevCrypto(token)
    tmp = crypto.decrypt(buf[UID_SIZE:])
    return json.loads(tmp)
