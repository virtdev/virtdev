#      remote.py
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

import os
import stat
import errno
import pyhdfs
from fuse import FuseOSError
from fi import VDevFileInterface
from snakebite.client import Client
from lib.log import log_get, log_err
from lib.util import DIR_MODE, FILE_MODE
from conf.virtdev import VDEV_NAMENODE_PORT

VDEV_FILE_SIZE = 1000000

class VDevRemoteFS(VDevFileInterface):
    def __init__(self, router):
        self._router = router
    
    def _get_client(self, uid, path):
        addr = self._router.get('dfs', uid)
        return Client(addr, VDEV_NAMENODE_PORT, use_trash=False)
    
    def _get_addr(self, uid, path):
        addr = self._router.get('dfs', uid)
        return 'hdfs://%s:%d%s' % (addr, VDEV_NAMENODE_PORT, path)
    
    def _truncate(self, uid, path, length):
        total = 0;
        addr = self._get_addr(uid, path)
        f = pyhdfs.open(addr, 'r')
        try:
            output = ''
            while length > 0:
                if length > 4096:
                    buf = f.read(4096)
                else:
                    buf = f.read(length)
                if not buf:
                    log_err(self, 'failed to truncate')
                    raise FuseOSError(errno.EINVAL)
                buflen = len(buf)
                if total + buflen >= VDEV_FILE_SIZE:
                    log_err(self, 'failed to truncate, invalid file size')
                    raise FuseOSError(errno.EINVAL)
                output += buf
                total += buflen
                length -= buflen
        finally:
            f.close()
        pyhdfs.delete(addr)
        f = pyhdfs.open(addr, 'w')
        try:
            f.write(buf)
        finally:
            f.close()
        return True
    
    def _rename(self, uid, src, dest):
        total = 0
        addr_src = self._get_addr(uid, src)
        addr_dest = self._get_addr(uid, dest)
        f_src = pyhdfs.open(addr_src, 'r')
        try:
            f_dest = pyhdfs.open(addr_dest, 'w')
            try:
                output = ''
                while True:
                    buf = f_src.read(4096)
                    if not buf:
                        break
                    total += len(buf)
                    if total >= VDEV_FILE_SIZE:
                        log_err(self, 'failed to rename, invalid file size')
                        raise FuseOSError(errno.EINVAL)
                    output += buf
                f_dest.write(output)
            finally:
                f_dest.close()
        finally:
            f_src.close()
        self.remove(uid, src)
        return True
    
    def load(self, uid, src, dest):
        cli = self._get_client(uid, src)
        ret = cli.copyToLocal([src], dest).next()
        if ret:
            return ret['result']
    
    def save(self, uid, src, dest):
        length = os.path.getsize(src)
        if length > VDEV_FILE_SIZE:
            log_err(self, 'failed to save, invalid file length')
            raise Exception(log_get(self, 'failed to save'))
        with open(src, 'r') as f:
            buf = f.read()
        addr = self._get_addr(uid, dest)
        f = pyhdfs.open(addr, 'w')
        if not f:
            log_err(self, 'failed to save')
            raise FuseOSError(errno.EINVAL)
        try:
            f.write(buf)
        finally:
            f.close()
        return True
    
    def remove(self, uid, path):
        cli = self._get_client(uid, path)
        ret = cli.delete([path], recurse=True).next()
        if ret:
            return ret['result']
    
    def mkdir(self, uid, path):
        cli = self._get_client(uid, path)
        ret = cli.mkdir([path], create_parent=True).next()
        if ret:
            return ret['result']
    
    def lsdir(self, uid, path):
        ret = []
        cli = self._get_client(uid, path)
        for name in cli.ls([path]):
            ret.append(os.path.basename(name['path']))
        return ret
    
    def exists(self, uid, path):
        cli = self._get_client(uid, path)
        return cli.test(path, exists=True)
    
    def touch(self, uid, path):
        cli = self._get_client(uid, path)
        ret = cli.touchz([path]).next()
        if ret:
            return ret['result']
    
    def rename(self, uid, src, dest):
        if self._namenode.get(uid, src) == self._namenode.get(uid, dest):
            cli = self._get_client(uid, src)
            ret = cli.rename([src], dest).next()
            if ret:
                return ret['result']
        else:
            return self._rename(uid, src, dest)
    
    def stat(self, uid, path):
        cli = self._get_client(uid, path)
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
            cli = self._get_client(uid, path)
            ret = cli.delete([path]).next()
            if ret and ret['result']:
                ret = cli.touchz([path]).next()
                if ret:
                    return ret['result']
        elif length > 0:
            return self._truncate(uid, path, length)
    
    def mtime(self, uid, path):
        cli = self._get_client(uid, path)
        st = cli.stat([path])
        if st:
            return st['modification_time']
    