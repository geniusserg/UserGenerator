from dostoevsky.tokenization import RegexTokenizer
from dostoevsky.models import FastTextSocialNetworkModel
import pandas as pd

tokenizer = RegexTokenizer()
model = FastTextSocialNetworkModel(tokenizer=tokenizer)

def sentiment_analysis(messages):
    results = model.predict(messages, k=2)
    sentiment_result = []
    for message, sentiment in zip(messages, results):
        sentiment_result.append(sentiment)
    sentiment = pd.DataFrame(sentiment_result)
    return sentiment
