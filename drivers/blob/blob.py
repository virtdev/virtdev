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

from base64 import b64decode
from textblob import TextBlob
from dev.driver import Driver, check_output

PRINT = False

class Blob(Driver):
    def _get_sentiment(self, text):
        buf = b64decode(text)
        if buf:
            blob = TextBlob(buf.decode('utf8'))
            polarity = blob.sentiment.polarity
            subjectivity = blob.sentiment.subjectivity
            if PRINT:
                print('Blob: polarity=%f, subjectivity=%f' % (polarity, subjectivity))
            return (polarity,  subjectivity)
        else:
            return (None, None)
    
    @check_output
    def put(self, args):
        text = args.get('content')
        if text:
            polarity, subjectivity = self._get_sentiment(text)
            if polarity != None and subjectivity != None:
                return {'polarity':polarity, 'subjectivity':subjectivity}
