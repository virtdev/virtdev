# downloader.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

import os
import wget
from threading import Thread
from lib.util import readlink
from dev.driver import Driver, check_output

PRINT = False
HOME = "~/vdev/dev/downloader"

class Downloader(Driver):
    def setup(self):
        path = self._get_path()
        os.system('mkdir -p %s' % path)
    
    def _get_path(self):
        return readlink(HOME)
    
    def _do_download(self, url):
        try:
            filename = wget.download(url, out=self._get_path(), bar=None)
            if PRINT:
                print('Downloader: filename=%s' % str(filename))
        except:
            if PRINT:
                print('Downloader: failed to download')
    
    def _download(self, url):
        Thread(target=self._do_download, args=(url,)).start()
        return True
    
    @check_output
    def put(self, args):
        url = args.get('url')
        if url:
            if self._download(url):
                return {'enable':'true'}
