from dostoevsky.tokenization import RegexTokenizer
from dostoevsky.models import FastTextSocialNetworkModel
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from gensim import corpora, models
from nltk.corpus import stopwords
rus_stopwords = stopwords.words("russian")

tokenizer = RegexTokenizer()
model = FastTextSocialNetworkModel(tokenizer=tokenizer)

def sentiment_analysis(messages):
    results = model.predict(messages, k=2)
    sentiment_result = []
    for message, sentiment in zip(messages, results):
        sentiment_result.append(sentiment)
    sentiment = pd.DataFrame(sentiment_result)
    return sentiment

def tfidf_process_texts(texts):
    vectorizer = TfidfVectorizer(max_features=1000, stop_words=rus_stopwords)
    tfidf_matrix = vectorizer.fit_transform(texts)
    feature_names = vectorizer.get_feature_names_out()
    tfidf_vectors = [{feature_names[i]: value for i, value in enumerate(row.data)}
                     for row in tfidf_matrix]
    return tfidf_vectors

def lda_on_texts(texts, num_topics=5):
    tokenized_texts = [text.split() for text in texts]
    dictionary = corpora.Dictionary(tokenized_texts)
    corpus = [dictionary.doc2bow(text) for text in tokenized_texts]
    lda_model = models.LdaModel(corpus, num_topics=num_topics, id2word=dictionary, passes=15)
    topics_for_texts = []
    for i, text in enumerate(tokenized_texts):
        topics = lda_model[dictionary.doc2bow(text)]
        topics_for_texts.append(topics)
    return topics_for_texts

def extract_hashtags(text):
    hashtag_list = []
    for word in text.split():
        if word[0] == '#':
            hashtag_list.append(word[1:])
    return hashtag_list