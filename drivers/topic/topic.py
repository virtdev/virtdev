#      topic.py
#      
#      Copyright (C) 2016 Yi-Wei Ci <ciyiwei@hotmail.com>
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
from gensim import corpora, models
from stop_words import get_stop_words
from nltk.tokenize import RegexpTokenizer
from nltk.stem.porter import PorterStemmer
from dev.driver import Driver, check_output

PRINT = False
NR_WORDS = 4
NR_TOPICS = 1
EN_STOP = get_stop_words('en')

class Topic(Driver):
    def _get_topics(self, text):
        buf = b64decode(text)
        if buf:
            tokenizer = RegexpTokenizer(r'\w+')
            poster_stemmer = PorterStemmer()
            doc = buf.decode('utf8')
            raw = doc.lower()
            tokens = tokenizer.tokenize(raw)
            stopped_tokens = [i for i in tokens if not i in EN_STOP]
            stemmed_tokens = [poster_stemmer.stem(i) for i in stopped_tokens]
            dictionary = corpora.Dictionary([stemmed_tokens])
            corpus = [dictionary.doc2bow(stemmed_tokens)]
            ldamodel = models.ldamodel.LdaModel(corpus, num_topics=NR_TOPICS, id2word=dictionary, passes=20)
            topics = str(ldamodel.print_topics(num_topics=NR_TOPICS, num_words=NR_WORDS))
            if PRINT:
                print('Topic: topics=%s' % topics)
            return topics
    
    @check_output
    def put(self, args):
        text = args.get('content')
        if text:
            topics = self._get_topics(text)
            if topics:
                return {'topics':topics}
