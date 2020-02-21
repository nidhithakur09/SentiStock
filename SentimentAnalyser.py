import time
import pandas as pd
import json
import pickle
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn import svm
from sklearn.metrics import classification_report
from string import punctuation
from nltk.corpus import stopwords 
stop_words = set(stopwords.words('english')) 
import json

data = open("config.json", "r")
config = json.load(data)


vectorizer = pickle.load(open("vectorizer.sav", "rb"))
classifier = pickle.load(open("classifier.sav", "rb"))


def classify(title):
    temp = title.lower().strip()
    temp = ''.join([x for x in temp if x not in punctuation])
    temp = ''.join(i for i in temp if not i.isdigit())
    words=temp.split()
    nw=""
    for r in words: 
        if not r in stop_words: 
            nw+=" "+r
    temp=nw.strip()
    line_vector = vectorizer.transform([temp])
    result = classifier.predict(line_vector)[0]
    return result


def train():
    global vectorizer
    global classifier
    vectorizer = TfidfVectorizer(
        min_df=5, max_df=0.8, sublinear_tf=True, use_idf=True)
    data = pd.read_csv("cleaned_data.csv",index_col=False,skip_blank_lines=True,sep=r'\s*,\s*').dropna() 
    data = data.sample(frac=1).reset_index(drop=True)
    data.head()
    print(data.head())
    L=len(data)
    print(L)
    train_index = int(0.60 * L)
    train = data[:train_index]
    train_vectors = vectorizer.fit_transform(train['title'])
    test = data[train_index:]
    test_vectors = vectorizer.transform(test['title'])
    classifier_linear = svm.SVC(kernel='linear')
    print("Training Start :",time.ctime())
    classifier_linear.fit(train_vectors, train['sentiment'])
    print("Training End :",time.ctime())
    print("Testing Start :",time.ctime())
    prediction_linear = classifier_linear.predict(test_vectors)
    print("Testing Start :",time.ctime())
    report = classification_report(
        test['sentiment'], prediction_linear, output_dict=True)
    print(report)
    with open("precision_report.txt","w") as file1:
        file1.write(json.dumps(report))
        file1.close()
    pickle.dump(vectorizer, open('vectorizer.sav', 'wb'))
    pickle.dump(classifier_linear, open('classifier.sav', 'wb'))
    return 0

# x= classify("Coffee Day stock dives 20% after founder goes missing")
# print(x)