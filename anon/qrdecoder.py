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
from StringIO import StringIO
from dev.anon import VDevAnon
from base64 import decodestring

DEBUG_QRDECODER = False

class QRDecoder(VDevAnon):    
    def decode(self, image):
        buf = decodestring(image)
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
        args = self._get_args(buf)
        if args and type(args) == dict:
            name = args.get('Name')
            image = args.get('Image')
            if image:
                url = self.decode(image)
                if url:
                    if DEBUG_QRDECODER:
                        print('QRDecoder: url=%s' % url)
                    if name:
                        return {'Name':name, 'URL':url}
                    else:
                        return {'URL':url}
    
if __name__ == '__main__':
    import pyqrcode
    from base64 import encodestring
    path_png = '/tmp/qr.png'
    path_jpg = '/tmp/qr.jpg'
    qr = pyqrcode.create('hello')
    qr.png(path_png, scale=6)
    image = Image.open(path_png)
    image.save(path_jpg)
    dec = QRDecoder()
    with open(path_jpg) as f:
        buf = f.read()
    image = encodestring(buf)
    ret = dec.decode(image)
    print('QRDecoder: ret=%s' % str(ret))
    