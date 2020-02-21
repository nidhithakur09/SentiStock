File Structure :
SentiStock.py - Main driver code to fetch news and to perform basic preprocessing on the news.
SentimentAnalyser.py - File used to return predicted sentiment of a News Title.
NewsCategory.py - Classifies the news into categories.
SymbolExtract.py - Extracts Symbols from the news title to find if any specific Stock/Company has been mentioned.
vectorizer.sav - vectorizer model for SentimentAnalyser
classifer.sav - classifier model for SentimentAnalyser
cat_vectorizer - vectorizer model for NewsCategory
cat_classifier.sav - vectorizer model for NewsCategory


installation commands:

pip3 install -r requirements.txt

or

pip install -r requirements.txt

Running Commands:
python3 SentiStock.py

Make sure there is a stable internet connectiong when Running.
output will be shown directly on the terminal. Use ctrl+c to stop execution and see the output