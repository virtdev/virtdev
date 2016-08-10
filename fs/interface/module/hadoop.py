# hadoop.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
import stat
from lib.domains import DOMAIN_USR
from lib.log import log_get, log_err
from lib.util import DIR_MODE, FILE_MODE
from hdfs.client import Client as HDFSClient
from snakebite.client import Client as SnakebiteClient
from conf.meta import FILE_SERVER_PORT, FILE_HTTP_PORT

FILE_SIZE = 1 << 24

class Hadoop(object):
    def __init__(self, router):
        self._router = router
    
    def _get_client(self, uid):
        addr = self._router.get(uid, DOMAIN_USR)
        self._log('get_client, addr=%s, uid=%s' % (addr, uid))
        return SnakebiteClient(addr, FILE_SERVER_PORT, use_trash=False)
    
    def _get_http_client(self, uid):
        addr = self._router.get(uid, DOMAIN_USR)
        self._log('get_http_client, addr=%s, uid=%s' % (addr, uid))
        address = 'http://%s:%d' % (addr, FILE_HTTP_PORT)
        return HDFSClient(address)
    
    def _truncate(self, uid, path, length):
        buf = ''
        cli = self._get_http_client(uid)
        if length > 0:
            with cli.read(path, length=length) as reader:
                buf = reader.read()
        with cli.write(path, overwrite=True) as writer:
            writer.write(buf)
        return True
    
    def load(self, uid, src, dest):
        cli = self._get_http_client(uid)
        cli.download(src, dest, overwrite=True)
        return True
    
    def save(self, uid, src, dest):
        length = os.path.getsize(src)
        if length > FILE_SIZE:
            log_err(self, 'failed to save')
            raise Exception(log_get(self, 'failed to save'))
        with open(src, 'r') as f:
            buf = f.read()
        cli = self._get_http_client(uid)
        with cli.write(dest, overwrite=True) as writer:
            writer.write(buf) 
        return True
    
    def remove(self, uid, path):
        cli = self._get_client(uid)
        ret = cli.delete([path], recurse=True).next()
        if ret:
            return ret['result']
    
    def mkdir(self, uid, path):
        cli = self._get_client(uid)
        ret = cli.mkdir([path], create_parent=True).next()
        if ret:
            return ret['result']
    
    def lsdir(self, uid, path):
        ret = []
        cli = self._get_client(uid)
        for name in cli.ls([path]):
            ret.append(os.path.basename(name['path']))
        return ret
    
    def exists(self, uid, path):
        cli = self._get_client(uid)
        return cli.test(path, exists=True)
    
    def touch(self, uid, path):
        cli = self._get_client(uid)
        cli.touchz([path]).next()
    
    def rename(self, uid, src, dest):
        cli = self._get_client(uid)
        ret = cli.rename([src], dest).next()
        if ret:
            return ret['result']
    
    def stat(self, uid, path):
        cli = self._get_client(uid)
        st = cli.stat([path])
        ret = {}
        if st:
            ret.update({'st_atime':st['access_time'] / 1000})
            ret.update({'st_mtime':st['modification_time'] / 1000})
            ret.update({'st_size':st['length']})
            ret.update({'st_nlink':1})
            if st['file_type'] == 'd':
                ret.update({'st_mode':stat.S_IFDIR | DIR_MODE})
            else:
                ret.update({'st_mode':stat.S_IFREG | FILE_MODE})
            return ret
    
    def truncate(self, uid, path, length):
        if 0 == length:
            cli = self._get_client(uid)
            ret = cli.delete([path]).next()
            if ret and ret['result']:
                ret = cli.touchz([path]).next()
                if ret:
                    return ret['result']
        elif length > 0:
            return self._truncate(uid, path, length)
    
    def mtime(self, uid, path):
        cli = self._get_client(uid)
        st = cli.stat([path])
        if st:
            return st['modification_time']
