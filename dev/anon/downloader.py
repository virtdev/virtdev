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

import md5
import wget
from threading import Thread
from aop import VDevAnonOper

class Downloader(VDevAnonOper):    
    def _do_download(self, url):
        filename = wget.download(url)
        print 'Downloader: filename=%s' % str(filename)
        
    def download(self, url):
        Thread(target=self._do_download, args=(url,)).start()
        return True
    
    def put(self, buf):
        args = self._get_args(buf)
        if args and type(args) == dict:
            url = args.get('String')
            if url:
                if self.download(url):
                    return {'String':md5.new(url).hexdigest()}
        else:
            print 'Downloader: invalid args'
    