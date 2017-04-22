#
# Copyright (C) 2016 Yi-Wei Ci
#
# Distributed under the terms of the MIT license.
#

from base64 import b64decode
from gensim import corpora, models
from stop_words import get_stop_words
from dev.driver import Driver, wrapper
from nltk.tokenize import RegexpTokenizer
from nltk.stem.porter import PorterStemmer

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

	@wrapper
	def put(self, *args, **kwargs):
		text = kwargs.get('content')
		if text:
			topics = self._get_topics(text)
			if topics:
				return {'topics':topics}
