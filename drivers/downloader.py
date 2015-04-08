#      downloader.py
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
import wget
from lib.log import log_err
from threading import Thread
from dev.driver import VDevDriver
from lib.mode import MODE_IN, MODE_VISI

DEBUG_DOWNLOADER = False
PATH_DOWNLOADER = "/opt/downloads"

class Downloader(VDevDriver):
    def __init__(self, name=None, sock=None):
        VDevDriver.__init__(self, name, sock)
        if not os.path.exists(PATH_DOWNLOADER):
            os.makedirs(PATH_DOWNLOADER, 0o755)
    
    def _do_download(self, url):
        try:
            filename = wget.download(url, out=PATH_DOWNLOADER, bar=None)
            if DEBUG_DOWNLOADER:
                print('Downloader: filename=%s' % str(filename))
        except:
            log_err(self, 'failed to download')
    
    def _download(self, url):
        Thread(target=self._do_download, args=(url,)).start()
        return True
    
    def info(self):
        return {'mode':MODE_IN | MODE_VISI}
    
    def put(self, buf):
        args = self.get_args(buf)
        if args and type(args) == dict:
            url = args.get('URL')
            name = args.get('Name')
            if url:
                if self._download(url):
                    if name:
                        ret = {'Name':name, 'Enable':'True'}
                    else:
                        ret = {'Enable':'True'}
                    timer = args.get('Timer')
                    if timer:
                        ret.update({'Timer':timer})
                    return ret
                    
        else:
            if DEBUG_DOWNLOADER:
                print('Downloader: invalid args')
