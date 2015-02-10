#      blob.py
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

import html2text
from urllib import urlopen
from base64 import encodestring

import sys
sys.path.append('..')
from anon.blob import Blob

PATH_TEXT = '/tmp/blob'
URL = 'http://en.wikipedia.org/wiki/User:West.andrew.g/Popular_pages'

if __name__ == '__main__':
    blob = Blob()
    h = html2text.HTML2Text()
    h.ignore_links = True
    html = urlopen(URL).read()
    result = h.handle(html.decode('utf8'))
    buf = result.encode('utf8')
    with open(PATH_TEXT, 'w') as f:
        f.write(buf)
    text = encodestring(buf)
    ret = blob.get_sentiment(text)
    print('Blob: ret=%s' % str(ret))
