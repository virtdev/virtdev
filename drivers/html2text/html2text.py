#      html2text.py
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

import html2text
from base64 import b64encode, b64decode
from dev.driver import Driver, check_output

PRINT = False
IGNORE_LINKS = True
IGNORE_IMAGES = True

class HTML2Text(Driver):    
    def _convert(self, html):
        buf = b64decode(html)
        if buf:
            try:
                temp = buf.decode(errors='ignore')
                h = html2text.HTML2Text()
                if IGNORE_LINKS:
                    h.ignore_links = True
                if IGNORE_IMAGES:
                    h.ignore_images = True
                res = h.handle(temp)
                if res:
                    return res.encode('utf-8')
            except:
                if PRINT:
                    print('HTML2Text: failed to convert')
    
    @check_output
    def put(self, args):
        html = args.get('content')
        if html:
            text = self._convert(html)
            if text:
                return {'content':b64encode(text)}
