#      qrdecoder.py
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

import zbar
import Image
from base64 import b64decode
from dev.driver import Driver
from StringIO import StringIO

PRINT = False

class QRDecoder(Driver):    
    def _decode(self, image):
        buf = b64decode(image)
        if buf:
            f = StringIO(buf)
            src = Image.open(f)
            w, h = src.size
            raw = src.tostring()
            scanner = zbar.ImageScanner()
            scanner.parse_config('enable')
            img = zbar.Image(w, h, 'Y800', raw)
            scanner.scan(img)
            for symbol in img:
                if str(symbol.type) == 'QRCODE':
                    return str(symbol.data).lower()
    
    def put(self, buf):
        args = self.get_args(buf)
        if args and type(args) == dict:
            image = args.get('content')
            if image:
                url = self._decode(image)
                if url:
                    if PRINT:
                        print('QRDecoder: url=%s' % url)
                    ret = {'url':url}
                    name = args.get('name')
                    if name:
                        ret.update({'name':name})
                    timer = args.get('timer')
                    if timer:
                        ret.update({'timer':timer})
                    return ret
