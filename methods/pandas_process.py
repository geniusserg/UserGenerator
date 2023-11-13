from methods.nlp_analysis import sentiment_analysis, tfidf_process_texts, lda_on_texts, extract_hashtags
import pandas as pd
import numpy as np

# [{"post_id": 000, "owner": 000, ....}]
def posts_transfrom_json_to_pandas(response):
    df = pd.json_normalize(response)
    # date to pandas date
    for i in ["date", "photo_date", "video_date"]:
        if (i in df.columns):
            df.loc[:, i] = pd.to_datetime(df[i], unit='s', origin='unix') 
    #sentiment
    sentiment_result = sentiment_analysis(df["text"])
    df.loc[:, sentiment_result.columns] = sentiment_result
    #hashtags
    df.loc[:, "hashtags"] = df["text"].apply(extract_hashtags)
    #tfidf
    df.loc[:, "tfidf"] = tfidf_process_texts(df["text"])
    #lda
    df.loc[:, "lda"] = lda_on_texts(df["text"], num_topics=5)

    # delete columns
    for col in ["carousel_offset"]:
        if (col in df.columns):
            df = df.drop(columns=col)
    return df

# [{id: 000, name:Ivan, ...}]
def users_transfrom_json_to_pandas(response):
    uinfodf = pd.json_normalize(response)
    if ("relation_partner.id" in uinfodf.columns):
        uinfodf.loc[:, "has_relations"] = (~uinfodf["relation_partner.id"].isna()).astype(int)
    if ("relation" in uinfodf.columns):
        uinfodf.loc[~pd.isna(uinfodf["relation"]) & (uinfodf["relation"]==0), "relation"] = None
    for col in ["career", "military", "schools", "universities"]:
        if(col in uinfodf.columns):
            # eliminate [] chagning to NaN and take last value in array
            uinfodf.loc[~uinfodf[col].isna(), col] = uinfodf.loc[~uinfodf[col].isna(), col].apply(lambda x: x[-1] if (not isinstance(x, float) and len(x) > 0 ) else pd.NA)
            # normalize field
            vcarer = pd.json_normalize(uinfodf.loc[~uinfodf[col].isna(), col])
            # copy transformed values to needed fields in dataframe
            uinfodf.loc[~uinfodf[col].isna(), [f"{col}__{i}" for i in vcarer.columns]] = vcarer.to_numpy().copy()
            uinfodf = uinfodf.drop(col, axis=1)
    def bdate_year_parser(bday):
        if pd.isna(bday):
            return None
        bday = bday.split(".")
        return pd.to_datetime(".".join(bday), format="%d.%m.%Y") if (len(bday) == 3) else None
    uinfodf.loc[:, "bdate"] = uinfodf["bdate"].apply(bdate_year_parser)
    now = pd.to_datetime('now')
    uinfodf.loc[~uinfodf["bdate"].isna(), 'age'] = uinfodf.loc[~uinfodf["bdate"].isna(), 'bdate'].apply(lambda x: (now.year - x.year) - ((now.month - x.month) < 0))

    # delete columns
    for col in ["can_access_closed", 
                "relation_partner.id", 
                'relation_partner.first_name', 
                'relation_partner.last_name',
                'personal.langs_full']:
        if (col in uinfodf.columns):
            uinfodf = uinfodf.drop(columns=col)
    return uinfodf

# [{post_id:123, likes:[132, 12, 4, 14, ...], ...}]
def likes_to_recsys_matrix(response):
    dfres = []
    for i in response:
        postid = i["post_id"]
        for j in i["likes"]:
            dfres.append([postid, j])
    df = pd.DataFrame(dfres)
    sparse_matrix = df.pivot_table(index=0, columns=1, aggfunc="size") # remove later, memory consuming
    return sparse_matrix