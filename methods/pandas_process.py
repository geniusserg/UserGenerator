from methods.sentiment_analysis import sentiment_analysis
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from gensim import corpora, models
from nltk.corpus import stopwords
rus_stopwords = stopwords.words("russian")

def extract_hastags(text):
    hashtag_list = []
    for word in text.split():
        if word[0] == '#':
            hashtag_list.append(word[1:])
    return hashtag_list

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

def posts_transfrom_json_to_pandas(response_dict):
    df = pd.json_normalize([{"post_id": i["post_id"], **i["post_info"]} for i in response_dict["posts"]])
    # date to pandas date
    for i in ["date", "photo_date", "video_date"]:
        df.loc[:, i] = pd.to_datetime(df[i], unit='s', origin='unix') 
    #sentiment
    sentiment_result = sentiment_analysis(df["text"])
    df.loc[:, sentiment_result.columns] = sentiment_result
    #hashtags
    df.loc[:, "hashtags"] = df["text"].apply(extract_hastags)
    #tfidf
    df.loc[:, "tfidf"] = tfidf_process_texts(df["text"])
    #lda
    df.loc[:, "lda"] = lda_on_texts(df["text"], num_topics=5)
    return df

def users_transfrom_json_to_pandas(response_dict):
    uinfodf = pd.json_normalize(response_dict)
    for col in ["career", "military"]:
        uinfodf.loc[~uinfodf[col].isna(), col] = uinfodf.loc[~uinfodf[col].isna(), col].apply(lambda x: x[-1] if (not isinstance(x, float) and len(x) > 0 ) else pd.NA)
        vcarer = pd.json_normalize(uinfodf[col])
        uinfodf.loc[:, [i+f"_{col}" for i in vcarer.columns]] = vcarer
        uinfodf = uinfodf.drop(col, axis=1)

def likes_to_recsys_matrix(response_dict):
    dfres = []
    for i in response_dict["likes"]:
        postid = i["post_id"]
        for j in i["likes"]:
            dfres.append([postid, j])
    df = pd.DataFrame(dfres)
    sparse_matrix = df.pivot_table(index=0, columns=1, aggfunc="size")
    return sparse_matrix