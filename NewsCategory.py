import time
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn import svm
from sklearn.metrics import classification_report
import mysql.connector
import pickle
import logging
import json
import numpy as np
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import re
stop_words = set(stopwords.words('english'))
LOG_FILENAME = 'log.out'
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)

# config = open("config.json", "r")
# config = json.load(config)

# mydb = mysql.connector.connect(
#     host="192.168.2.89",
#     user="presql",
#     passwd="presql",
#     database="Analytical"
# )

# #load the pre-trained models for vectorizer and classifier
cat_vectorizer = pickle.load(open("cat_vectorizer.sav", "rb"))
cat_classifier = pickle.load(open("cat_classifier.sav", "rb"))

# #function call to return category of given news title
def categorize(title):
    temp = title.lower()
    temp = temp.strip()
    line_vector = cat_vectorizer.transform([temp])
    result = cat_classifier.predict(line_vector)[0]
    return result

# #Retrain models with updated news
def cat_train():
    global cat_vectorizer
    global cat_classifier
# data = pd.read_sql('SELECT rn_title,rn_category FROM ' +
#                     config["sql"]["train_data"], con=mydb)
    data = pd.read_csv("cat_train.csv")
    cat_vectorizer = TfidfVectorizer(
        min_df=5, max_df=0.8, sublinear_tf=True, use_idf=True)
    L = len(data)
    train_index = int(0.60 * L)
    train = data
    train_vectors = cat_vectorizer.fit_transform(train['Title'])
    test = data[train_index:]
    test_vectors = cat_vectorizer.transform(test['Title'])
    labels = data['Category'].tolist()
    labels = np.array(labels)
    labels = np.unique(labels)
    cat_classifier = svm.SVC(kernel='linear')
    logging.debug("Categorizer Training time start : "+time.ctime())
    cat_classifier.fit(train_vectors, train['Category'])
    logging.debug("Categorizer Training time end : "+time.ctime())
    logging.debug("Categorizer Testing time start : "+time.ctime())
    prediction_linear = cat_classifier.predict(test_vectors)
    logging.debug("Categorizer Testing time end : "+time.ctime())
    pickle.dump(cat_vectorizer, open('cat_vectorizer.sav', 'wb'))
    pickle.dump(cat_classifier, open('cat_classifier.sav', 'wb'))
    report = classification_report(
        test['Category'], prediction_linear, output_dict=True)
    print(report)
    return 0
# cat_train()