# localdb.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from lib.log import log_debug
from conf.log import LOG_LOCALDB
from conf.virtdev import PROT_LOCALDB

if PROT_LOCALDB == 'sophia':
    from module.sophiadb import SophiaDB as DB
elif PROT_LOCALDB == 'leveldb':
    from module.level import LevelDB as DB
else:
    raise Exception('Error: PROT_LOCALDB is not set')

class LocalDB(DB):
    def _log(self, text):
        if LOG_LOCALDB:
            log_debug(self, text)
