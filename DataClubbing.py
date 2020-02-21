import difflib
import configparser
import mysql.connector
from mysql.connector import Error
from fuzzywuzzy import fuzz
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import pandas as pd
# import pysolr

# solr = pysolr.Solr('http://localhost:8983/solr/Rupee_News', timeout=10)
# simsolr = pysolr.Solr('http://localhost:8983/solr/Rupee_News_All', timeout=10)

config = configparser.ConfigParser()
config.read('config.ini')

RATION_THRESHOLD = config.getint('RSS_DEFAULT', 'clubbing_matcher_ratio')


# for first time it need to uncomment once it download you can comment it
# nltk.download('stopwords')
# nltk.download('punkt')
# nltk.download('wordnet')

hashId = []
title = []
description = []
link = []
newsDate = []
imageSrc = []
sent = []
symbol = []
source = []
category = []
fullDescription = []
# topic = []

hashIdParent = []
hashIdChild = []
titleChild = []
descriptionChild = []
linkChild = []
newsDateChild = []
imageSrcChild = []
sentChild = []
symbolChild = []
sourceChild = []
categoryChild = []
fullDescriptionChild = []
# topicChild = []


def DataLoader():
    connection = mysql.connector.connect(host='localhost',
                                         database='SentiStock',
                                         user='root',
                                         password='12')
    #cursor = connection.cursor()
    #
    sql = "SELECT * FROM SentiStock.SS_News;"
    cursor = connection.cursor()
    cursor.execute(sql)
    records = cursor.fetchall()
    for row in records:
        hashId.append(row[0])
        source.append(row[1])
        link.append(row[2])
        newsDate.append(row[3])
        title.append(row[5])
        imageSrc.append(row[6])
        description.append(row[7])
        sent.append(row[8])
        symbol.append(row[9])
        category.append(row[10])
        fullDescription.append(row[11])
        # topic.append(row[15])
    cursor.close()
    # except Error as e:
    #     print("Error while connecting to MySQL", e)
    # finally:
    #     # closing database connection.
    #     if (connection.is_connected()):
    #         connection.close()
    #         print("mysql connection closed")


def DataLoaderForChild():
    connection = mysql.connector.connect(host='localhost',
                                         database='SentiStock',
                                         user='root',
                                         password='12')
    cursor = connection.cursor()
    try:
        sql = "SELECT * FROM SentiStock.SS_News_All;"
        cursor = connection.cursor()
        cursor.execute(sql)
        records = cursor.fetchall()
        for row in records:
            hashIdParent.append(row[0])
            hashIdChild.append(row[1])
            sourceChild.append(row[2])
            linkChild.append(row[3])
            newsDateChild.append(row[4])
            titleChild.append(row[6])
            imageSrcChild.append(row[7])
            descriptionChild.append(row[8])
            sentChild.append(row[9])
            symbolChild.append(row[10])
            categoryChild.append(row[11])
            fullDescriptionChild.append(row[12])
            # topicChild.append(row[15])
        cursor.close()
    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        # closing database connection.
        if (connection.is_connected()):
            connection.close()



def getCurrentDateTime():
    from datetime import datetime
    now = datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S")


# fillter tweets list
fillterTitle = []
Inserted = []
systemDate, systemTime = getCurrentDateTime().split()
systemY, systemM, systemD = systemDate.split('-')


def token_sort_ratio(str1, str2):
    return fuzz.token_sort_ratio(str1, str2)


def token_set_ratio(str1, str2):
    return fuzz.token_set_ratio(str1, str2)


def similar(a, b):
    return round(difflib.SequenceMatcher(None, a, b).ratio()*100, 1)


def CosineSimilarity(X, Y):
    # X = input("Enter first string: ").lower()
    # Y = input("Enter second string: ").lower()
    X = X.lower()
    Y = Y.lower()

    # print(X[:int(len(X)/2)])

    # tokenization
    X_list = word_tokenize(X)  # [:int(len(X) / 2)]
    Y_list = word_tokenize(Y)  # [:int(len(Y) / 2)]

    # sw contains the list of stopwords
    sw = stopwords.words('english')
    l1 = []
    l2 = []

    # remove stop words from string
    X_set = {w for w in X_list if not w in sw}
    Y_set = {w for w in Y_list if not w in sw}

    # form a set containing keywords of both strings
    rvector = X_set.union(Y_set)
    for w in rvector:
        if w in X_set:
            l1.append(1)  # create a vector
        else:
            l1.append(0)
        if w in Y_set:
            l2.append(1)
        else:
            l2.append(0)
    c = 0

    try:
        # cosine formula
        for i in range(len(rvector)):
            c += l1[i] * l2[i]
        cosine = c / float((sum(l1) * sum(l2)) ** 0.5)
        return round(cosine * 100)
    except ZeroDivisionError:
        return 0


def leftCosineSimilarity(X, Y):
    # X = input("Enter first string: ").lower()
    # Y = input("Enter second string: ").lower()
    X = X.lower()
    Y = Y.lower()

    # print(X[:int(len(X)/2)])

    # tokenization
    X_list = word_tokenize(X[:int(len(X) / 2)])
    Y_list = word_tokenize(Y[:int(len(Y) / 2)])

    # sw contains the list of stopwords
    sw = stopwords.words('english')
    l1 = []
    l2 = []

    # remove stop words from string
    X_set = {w for w in X_list if not w in sw}
    Y_set = {w for w in Y_list if not w in sw}

    # form a set containing keywords of both strings
    rvector = X_set.union(Y_set)
    for w in rvector:
        if w in X_set:
            l1.append(1)  # create a vector
        else:
            l1.append(0)
        if w in Y_set:
            l2.append(1)
        else:
            l2.append(0)
    c = 0

    try:
        # cosine formula
        for i in range(len(rvector)):
            c += l1[i] * l2[i]
        cosine = c / float((sum(l1) * sum(l2)) ** 0.5)
        return round(cosine * 100)
    except ZeroDivisionError:
        return 0


def rightCosineSimilarity(X, Y):
    # X = input("Enter first string: ").lower()
    # Y = input("Enter second string: ").lower()
    X = X.lower()
    Y = Y.lower()

    # print(X[:int(len(X)/2)])

    # tokenization
    X_list = word_tokenize(X[:int(len(X)//2):-1])
    Y_list = word_tokenize(Y[:int(len(Y)//2):-1])

    # sw contains the list of stopwords
    sw = stopwords.words('english')
    l1 = []
    l2 = []

    # remove stop words from string
    X_set = {w for w in X_list if not w in sw}
    Y_set = {w for w in Y_list if not w in sw}

    # form a set containing keywords of both strings
    rvector = X_set.union(Y_set)
    for w in rvector:
        if w in X_set:
            l1.append(1)  # create a vector
        else:
            l1.append(0)
        if w in Y_set:
            l2.append(1)
        else:
            l2.append(0)
    c = 0

    try:
        # cosine formula
        for i in range(len(rvector)):
            c += l1[i] * l2[i]
        cosine = c / float((sum(l1) * sum(l2)) ** 0.5)
        return round(cosine * 100)
    except ZeroDivisionError:
        return 0


def find_child_parent(listOfid):
    try:
        connection = mysql.connector.connect(host='localhost',
                                         database='SentiStock',
                                         user='root',
                                         password='12')
        cursor = connection.cursor()
        return_query = "select ss_parent_id FROM SS_News_All WHERE ss_news_id = '{}';".format(listOfid)
        cursor.execute(return_query)
        # connection.commit()
        recordss = cursor.fetchone()
        return recordss[0]
    except Error as e:
        print("Error : ", e)


def fetch_parent_data(parentID):
    try:
        connection = mysql.connector.connect(host='localhost',
                                         database='SentiStock',
                                         user='root',
                                         password='12')
        cursor = connection.cursor()
        return_query = "SELECT ss_title FROM SS_News WHERE ss_news_id = '{}';".format(parentID)
        cursor.execute(return_query)
        recordss = cursor.fetchone()
        return recordss[0]
    except Error as e:
        print("Error : ", e)



def findSimilarityInChild(titlefromnews, titlehashid, hashId, source, link, newsDate, time, title, imageSrc, description, sent, symbol, category,fulldescription):
    flag = False
    DataLoaderForChild()
    listOfRate = []
    listOfRate.append(0)
    rate = 0
    for i in range(0, len(titleChild)):
        rate = leftCosineSimilarity(titlefromnews, titleChild[i])
        listOfRate.append(rate)
    maxIndex = listOfRate.index(max(listOfRate))
    if max(listOfRate) >= 40:
        parend_id = find_child_parent(hashIdChild[maxIndex-1])
        parent_data = fetch_parent_data(parend_id)
        Arate = CosineSimilarity(titlefromnews, parent_data)
        if Arate >= 40:
            insertDataIntoSimilarNews(parend_id, hashId, source, link, newsDate, time, title, imageSrc, description, sent, symbol, category,fulldescription)
            Inserted.append(parend_id)
            flag = True
        else:
            pass

    listOfRate.clear()
    hashIdParent.clear()
    hashIdChild.clear()
    titleChild.clear()
    descriptionChild.clear()
    linkChild.clear()
    newsDateChild.clear()
    imageSrcChild.clear()
    sentChild.clear()
    symbolChild.clear()
    sourceChild.clear()
    categoryChild.clear()
    fullDescriptionChild.clear()
    # topicChild.clear()
    return flag


def ClubbSimilar(titlefromnews, titlehashid):
    print("entered1")
    flag = True
    falgg = True
    DataLoader()
    for i in range(0, len(title)):
        rate = leftCosineSimilarity(titlefromnews, title[i])
        #print("rate : ",rate)
        if rate == 100:
            pass
        elif rate >= RATION_THRESHOLD:
            for j in Inserted:
                if j == hashId[i]:
                    flag = False
                    break
                else:
                    flag = True
            if flag == True:
                rate = rightCosineSimilarity(titlefromnews, title[i])
                if rate >= RATION_THRESHOLD:
                    if(findSimilarityInChild(titlefromnews, titlehashid, hashId[i], source[i], link[i], newsDate[i], getCurrentDateTime(), title[i], imageSrc[i], description[i], sent[i], symbol[i], category[i],fullDescription[i]) == False):
                        if(insertDataIntoSimilarNews(titlehashid, hashId[i], source[i], link[i], newsDate[i], getCurrentDateTime(), title[i], imageSrc[i], description[i], sent[i], symbol[i], category[i],fullDescription[i])):
                            Inserted.append(titlehashid)
                    else:
                        pass
            else:
                pass
        else:
            pass

    title.clear()
    hashId.clear()
    description.clear()
    link.clear()
    newsDate.clear()
    imageSrc.clear()
    sent.clear()
    symbol.clear()
    source.clear()
    category.clear()
    fullDescription.clear()
    # topic.clear()


def hash_matcherSimilar(hashId):
    connection = mysql.connector.connect(host='localhost',
                                         database='SentiStock',
                                         user='root',
                                         password='12')
    mycursor = connection.cursor()
    hash_string = "SELECT ss_news_id FROM SS_News_All WHERE ss_news_id = '{}';".format(hashId)
    mycursor.execute(hash_string)
    recordss = mycursor.fetchall()
    if len(recordss) >= 1:
        return False
    else:
        return True


def insertDataIntoSimilarNews(Phash_id, hashId, source, link, newsDate, time, title, imageSrc, description, sent, symbol, category,fulldescription):
    connection = mysql.connector.connect(host='localhost',
                                         database='SentiStock',
                                         user='root',
                                         password='12')
    cursor = connection.cursor()
    if hash_matcherSimilar(hashId):
        clubbingInsertQuery = "INSERT INTO SS_News_All (ss_parent_id,ss_news_id,ss_source,ss_link,ss_time,ss_entry_time,ss_title,ss_image_link,ss_description,ss_sentiments,ss_symbol,ss_category,ss_full_description) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = (Phash_id, hashId, source, link, newsDate, time, title, imageSrc, description, sent, symbol, category,fulldescription)
        cursor.execute(clubbingInsertQuery, val)
        connection.commit()

        removeSimilarParentAndHashId()

        if(checkForParentInSimilar(Phash_id)):
            print("insider")
            get_data = "SELECT * FROM SentiStock.SS_News WHERE ss_news_id = '{}'".format(Phash_id)
            upload_data = "INSERT INTO SS_News_All (ss_news_id,ss_source,ss_link,ss_time,ss_entry_time,ss_title,ss_image_link,ss_description,ss_sentiments,ss_symbol,ss_category,ss_full_description) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(get_data)
            records = cursor.fetchall()
            for row in records:
                valONE = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11])
                cursor.execute(upload_data, valONE)
            connection.commit()




        get_query = "SELECT ss_match_count FROM SentiStock.SS_News WHERE ss_news_id = '{}'".format(Phash_id)
        cursor.execute(get_query)
        records = cursor.fetchall()
        records = records[0][0]

        sql_query = "update SentiStock.SS_News SET ss_match_count = '{}' WHERE ss_news_id = '{}'".format(records+1,Phash_id)
        cursor.execute(sql_query)
        connection.commit()



        # doc = {'rn_news_id': Phash_id, 'rn_match_count': records+1}
        # solr.add([doc],fieldUpdates={'rn_match_count':'set'})


        return True
    return False


def removeSimilarParentAndHashId():
    connection = mysql.connector.connect(host='localhost',
                                         database='SentiStock',
                                         user='root',
                                         password='12')
    cursor = connection.cursor()
    deleteQuery = "DELETE FROM SS_News_All WHERE ss_news_id = ss_parent_id;"
    cursor.execute(deleteQuery)
    connection.commit()


def checkForParentInSimilar(Phash_id):
    connection = mysql.connector.connect(host='localhost',
                                         database='SentiStock',
                                         user='root',
                                         password='12')
    mycursor = connection.cursor()
    hash_string = "SELECT * FROM SS_News_All WHERE ss_parent_id = '{}';".format(Phash_id)
    mycursor.execute(hash_string)
    recordss = mycursor.fetchall()
    if len(recordss) >= 1:
        return True
    else:
        return False
