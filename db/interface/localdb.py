#      localdb.py
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

from lib.log import log_debug
from conf.log import LOG_LOCALDB
from conf.prot import PROT_LOCALDB

if PROT_LOCALDB == 'sophia':
    from module.sophiadb import SophiaDB as DB
elif PROT_LOCALDB == 'leveldb':
    from module.level import LevelDB as DB

def LocalDB(DB):
    def _log(self, text):
        if LOG_LOCALDB:
            log_debug(self, text)
