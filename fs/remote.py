#      remote.py
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

from file import File
from conf.virtdev import HADOOP

class RemoteFile(File):
    def __init__(self, router):
        if HADOOP:
            from interface.hadoop import Hadoop
            self._file = Hadoop(router)
        else:
            self._file = None
    
    def load(self, uid, src, dest):
        if self._file:
            return self._file.load(uid, src, dest)
    
    def save(self, uid, src, dest):
        if self._file:
            return self._file.save(uid, src, dest)
    
    def remove(self, uid, path):
        if self._file:
            return self._file.remove(uid, path)
    
    def mkdir(self, uid, path):
        if self._file:
            return self._file.mkdir(uid, path)
    
    def lsdir(self, uid, path):
        if self._file:
            return self._file.lsdir(uid, path)
    
    def exists(self, uid, path):
        if self._file:
            return self._file.exists(uid, path)
    
    def touch(self, uid, path):
        if self._file:
            return self._file.touch(uid, path)
    
    def rename(self, uid, src, dest):
        if self._file:
            return self._file.rename(uid, src, dest)
    
    def stat(self, uid, path):
        if self._file:
            return self._file.stat(uid, path)
    
    def truncate(self, uid, path, length):
        if self._file:
            return self._file.truncate(uid, path, length)
    
    def mtime(self, uid, path):
        if self._file:
            return self._file.mtime(uid, path)
