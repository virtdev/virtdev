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
from threading import Lock
from fuse import FuseOSError
from fi import VDevFileInterface
from lib.log import log_get, log_err
from lib.util import DIR_MODE, FILE_MODE
from hdfs.client import Client as HTTPClient
from snakebite.client import Client as DFSClient
from conf.virtdev import VDEV_DFS_PORT, VDEV_DFS_HTTP_PORT

HTTP_TOUCH = True
HTTP_EXISTS = True
VDEV_FILE_SIZE = 1000000

class VDevRemoteFS(VDevFileInterface):
    def __init__(self, router):
        self._router = router
        
    def _get_dfs_cli(self, uid):
        addr = self._router.get('dfs', uid)
        return DFSClient(addr, VDEV_DFS_PORT, use_trash=False)
    
    def _get_http_cli(self, uid):
        addr = self._router.get('dfs', uid)
        http_addr = 'http://%s:%d' % (addr, VDEV_DFS_HTTP_PORT)
        return HTTPClient(http_addr)
    
    def _truncate(self, uid, path, length):
        cli = self._get_http_cli(uid)
        buf = cli.read(path, length=length).next()
        cli.write(buf, overwrite=True)
        return True
    
    def load(self, uid, src, dest):
        cli = self._get_http_cli(uid)
        cli.download(src, dest, overwrite=True)
        return True
    
    def save(self, uid, src, dest):
        length = os.path.getsize(src)
        if length > VDEV_FILE_SIZE:
            log_err(self, 'failed to save, invalid file length')
            raise Exception(log_get(self, 'failed to save'))
        with open(src, 'r') as f:
            buf = f.read()
        cli = self._get_http_cli(uid)
        cli.write(dest, buf, overwrite=True)
        return True
    
    def remove(self, uid, path):
        cli = self._get_dfs_cli(uid)
        ret = cli.delete([path], recurse=True).next()
        if ret:
            return ret['result']
    
    def mkdir(self, uid, path):
        cli = self._get_dfs_cli(uid)
        ret = cli.mkdir([path], create_parent=True).next()
        if ret:
            return ret['result']
    
    def lsdir(self, uid, path):
        ret = []
        cli = self._get_dfs_cli(uid)
        for name in cli.ls([path]):
            ret.append(os.path.basename(name['path']))
        return ret
    
    def _http_exists(self, uid, path):
        cli = self._get_http_cli(uid)
        try:
            cli.status(path)
            return True
        except:
            pass
        
    def _dfs_exists(self, uid, path):
        cli = self._get_dfs_cli(uid)
        return cli.test(path, exists=True)
    
    def exists(self, uid, path):
        if HTTP_EXISTS:
            return self._http_exists(uid, path)
        else:
            return self._dfs_exists(uid, path)
    
    def _http_touch(self, uid, path):
        cli = self._get_http_cli(uid)
        cli.write(path, "", overwrite=True)
        return True
    
    def _dfs_touch(self, uid, path):
        cli = self._get_dfs_cli(uid)
        ret = cli.touchz([path]).next()
        if ret:
            return ret['result']
    
    def touch(self, uid, path):
        if HTTP_TOUCH:
            self._http_touch(uid, path)
        else:
            self._dfs_touch(uid, path)
    
    def rename(self, uid, src, dest):
        cli = self._get_dfs_cli(uid)
        ret = cli.rename([src], dest).next()
        if ret:
            return ret['result']
    
    def stat(self, uid, path):
        cli = self._get_dfs_cli(uid)
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
            cli = self._get_dfs_cli(uid)
            ret = cli.delete([path]).next()
            if ret and ret['result']:
                ret = cli.touchz([path]).next()
                if ret:
                    return ret['result']
        elif length > 0:
            return self._truncate(uid, path, length)
    
    def mtime(self, uid, path):
        cli = self._get_dfs_cli(uid)
        st = cli.stat([path])
        if st:
            return st['modification_time']
    