# loader.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
import json
from lib.log import log_debug
from conf.log import LOG_LOADER
from lib.fields import FIELD_ATTR
from lib.attributes import ATTR_PROFILE
from lib.util import unicode2str, get_mnt_path

LOAD_MAX = 4096

class Loader(object):
    def __init__(self, uid):
        self._uid = uid
        self._mnt = get_mnt_path(self._uid)

    def _log(self, text):
        if LOG_LOADER:
            log_debug(self, text)

    def _get_path(self, name, attr):
        return os.path.join(self._mnt, FIELD_ATTR, name, attr)

    def _read(self, name, attr):
        ret = None
        path = self._get_path(name, attr)
        self._log('loading, name=%s, attr=%s, path=%s' % (str(name), str(attr), str(path)))
        try:
            fd = os.open(path, os.O_RDONLY)
            try:
                ret = os.read(fd, LOAD_MAX)
            finally:
                os.close(fd)
        except:
            pass

        if ret:
            if len(ret) == LOAD_MAX:
                log_debug(self, 'failed to load %s, invalid size' % str(name))
                return
            self._log('finished, name=%s, %s=%s' % (str(name), str(attr), str(ret)))
            return ret

    def get_attr(self, name, attr, typ):
        buf = self._read(name, attr)
        if buf:
            try:
                return typ(buf)
            except:
                log_debug(self, 'failed to convert %s to type %s' % (str(buf), str(typ)))

    def get_profile(self, name):
        buf = self._read(name, ATTR_PROFILE)
        if buf:
            attr = json.loads(buf)
            return unicode2str(attr)
