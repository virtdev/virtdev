# codec.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import struct
from lib import bson
from Crypto.Cipher import AES
from lib.log import log_err, log_get
from base64 import b64encode, b64decode
from lib.util import UID_SIZE, unicode2str

HEAD_SIZE = len(struct.pack('I', 0))

class Codec():
    def __init__(self, key):
        self._key = key[0:16]
        self._iv = key[16:32]

    def encode(self, buf):
        crypto = AES.new(self._key, AES.MODE_CBC, self._iv)
        ret = struct.pack('I', len(buf)) + buf
        ret += (16 - len(ret) % 16) * chr(0)
        return b64encode(crypto.encrypt(ret))

    def decode(self, buf):
        crypto = AES.new(self._key, AES.MODE_CBC, self._iv)
        ret = crypto.decrypt(b64decode(buf))
        if len(ret) < HEAD_SIZE:
            log_err(self, 'failed to decode')
            raise Exception(log_get(self, 'failed to decode'))
        length = struct.unpack('I', ret[0:HEAD_SIZE])[0]
        if len(ret) < HEAD_SIZE + length:
            log_err(self, 'failed to decode')
            raise Exception(log_get(self, 'failed to decode'))
        return ret[HEAD_SIZE:HEAD_SIZE + length]

def encode(buf, token, uid):
    codec = Codec(token)
    body = bson.dumps({'body':buf})
    return str(uid) + codec.encode(body)

def decode(buf, token, uid=None):
    length = len(buf)
    if length <= UID_SIZE or (uid and buf[0:UID_SIZE] != uid):
        return
    codec = Codec(token)
    body = codec.decode(buf[UID_SIZE:])
    return unicode2str(bson.loads(body)['body'])
