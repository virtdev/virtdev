# blob.py
#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

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
