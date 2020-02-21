import json
import re
import csv
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
stop_words = set(stopwords.words('english'))
filename = "cat_data2.json"
f = open(filename, "r")
data = json.load(f)
title = []
cat = []
title.append("Title")
cat.append("Category")
for i in range(0, len(data)):
    x = data[i]["Title"].lower()
    re.sub(r'.', '', x)
    x = re.sub('[^A-Za-z ]+', ' ', x)
    x = word_tokenize(x)
    filtered_sentence = [w for w in x if not w in stop_words]
    x = ' '.join(filtered_sentence)
    if(x in title):
        continue
    title.append(x)
    cat.append(data[i]["Categorys"][0]["CategoryName"])
print(len(data), len(title), len(cat))
with open('cat_train2.csv', 'w') as writeFile:
    writer = csv.writer(writeFile)
    for i in range(0, len(title)):
        temp = []
        temp.append(title[i])
        if(cat[i]=="NoCategory"):
            cat[i]="General"
        temp.append(cat[i])
        writer.writerow(temp)
writeFile.close()
