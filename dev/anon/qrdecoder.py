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

import PIL
import zbar
from aop import VDevAnonOper
from StringIO import StringIO
from base64 import decodestring

class QRDecoder(VDevAnonOper):    
    def decode(self, image):
        buf = decodestring(image)
        if buf:
            f = StringIO(buf)
            src = PIL.Image.open(f)
            w, h = src.size
            raw = src.tostring()
            scanner = zbar.ImageScanner()
            scanner.parse_config('enable')
            img = zbar.Image(w, h, 'Y800', raw)
            scanner.scan(img)
            for symbol in img:
                if str(symbol.type) == 'QRCODE':
                    return symbol.data
    
    def put(self, buf):
        args = self._get_args(buf)
        if args and type(args) == dict:
            name = args.get('Name')
            image = args.get('Image')
            if image:
                url = self.decode(image)
                if url:
                    print('QRDecoder: url=%s' % url)
                    if name:
                        return {'Name':name, 'URL':url}
                    else:
                        return {'URL':url}
    