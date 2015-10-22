#      kwget.py
#      
#      Copyright (C) 2015 Yi-Wei Ci <ciyiwei@hotmail.com>
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
from RAKE import Rake
from base64 import b64decode
from dev.driver import Driver

PRINT = False
STOPWORDS = 'stoplist'

class KWGet(Driver):
    def setup(self):
        path = os.path.join(os.path.dirname(__file__), STOPWORDS)
        self._rake = Rake(path)
    
    def _extract_keywords(self, text):
        buf = b64decode(text)
        return self._rake.run(buf)
    
    def put(self, buf):
        args = self.get_args(buf)
        if args and type(args) == dict:
            text = args.get('content')
            keywords = self._extract_keywords(text)
            if PRINT:
                print('KWGet: keywords=%s' % str(keywords))
            ret = {'keywords':keywords}
            name = args.get('name')
            if name:
                ret.update({'name':name})
            timer = args.get('timer')
            if timer:
                ret.update({'timer':timer})
            return ret
