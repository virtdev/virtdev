#      local.py
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
import shutil
from file import File
from subprocess import call
from lib.util import DEVNULL, DIR_MODE

class LocalFile(File):
    def load(self, uid, src, dest):
        call(['rsync', '-a', src, dest], stderr=DEVNULL, stdout=DEVNULL)
        return True
    
    def save(self, uid, src, dest):
        call(['rsync', '-a', src, dest], stderr=DEVNULL, stdout=DEVNULL)
        return True
    
    def remove(self, uid, path):
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.exists(path):
            os.remove(path)
        return True
    
    def mkdir(self, uid, path):
        os.makedirs(path, mode=DIR_MODE)
        return True
    
    def lsdir(self, uid, path):
        return os.listdir(path)
    
    def exists(self, uid, path):
        return os.path.exists(path)
    
    def touch(self, uid, path):
        open(path, 'a').close()
        return True
    
    def rename(self, uid, src, dest):
        os.rename(src, dest)
        return True
    
    def stat(self, uid, path):
        st = os.lstat(path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime', 'st_mode', 'st_mtime', 'st_nlink', 'st_size'))
    
    def truncate(self, uid, path, length):
        with open(path, 'r+') as f:
            f.truncate(length)
        return True
    
    def mtime(self, uid, path):
        return 0