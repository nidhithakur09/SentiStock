import requests
import xml.etree.ElementTree as ET
import re
import unicodedata
import mysql.connector
import time
import hashlib
import SentimentAnalyser as sa
import SymbolExtract as se
import Attention as Att
import dateparser
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

from DataClubbing import *

info = pd.read_csv("urls.csv")
url = info['Link']
source = info['Source']
df_cols = ['title', 'description', 'source', 'link',
           'date', 'imagesrc', 'sent', 'symbol', 'category']
out_df = pd.DataFrame(columns=df_cols)
start_time = time.time()

<<<<<<< HEAD
<<<<<<< HEAD
c_time = datetime.now().time()
today8am = c_time.replace(hour=8,minute=0,second=0,microsecond=0)
if c_time < today8am:
    Att.update()


=======
>>>>>>> 72fe77d2272513ea11e67ae347a712ee4e95678e
=======
>>>>>>> 72fe77d2272513ea11e67ae347a712ee4e95678e
def getCurrentDateTime():
    from datetime import datetime
    now = datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S")

<<<<<<< HEAD
<<<<<<< HEAD
def html_parser_title(title_link):
    url_get = requests.get(link)
    soup = BeautifulSoup(url_get.content, 'lxml')
    soup.find_all('div', {'class': 'brk_wraper clearfix'})
    return soup.select('h1')[0].text


def html_parser_description(title_link):
    url_get = requests.get(link)
    soup = BeautifulSoup(url_get.content, 'lxml')
    soup.find_all('div', {'class': 'brk_wraper clearfix'})
    return soup.select('h2')[0].text
=======
>>>>>>> 72fe77d2272513ea11e67ae347a712ee4e95678e
=======
>>>>>>> 72fe77d2272513ea11e67ae347a712ee4e95678e

def removeTag(string):
    return re.sub('<[A-Za-z\/][^>]*>', '', string)

def getCurrentDateTime():
    from datetime import datetime
    now = datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S")

def hash_matcher(hashID):
    mydb = mysql.connector.connect(host="192.168.2.89",
                                   user="uatmysql",
                                   passwd="uatmysql",
                                   database="PythonNews")

    mycursor = mydb.cursor()

    hash_string = "SELECT hash_id FROM news2 WHERE hash_id = '{}';".format(
        hashID)
    mycursor.execute(hash_string)
    records = mycursor.fetchall()
    if len(records) >= 1:
        # print(hashID)
        return False
    else:
        return True


count = 0
print(len(url))  # 79
flag = 0
for i in range(70, len(url)):
    filename = str("economic.xml")
    response = requests.get(url[i])

    with open(filename, 'wb') as f:
        f.write(response.content)
    try:
        tree = ET.parse(filename)
        root = tree.getroot()
    except:
        print("An error occured with url ", url[i])
    else:
        newsdata = []
        for item in root.findall('./channel/item'):
            # empty news dictionary
            category = info['Category'][i]
            source = info['Source'][i]
            for child in item:
                if child.tag == 'title':
                    title = child.text if child is not None else None
                elif child.tag == 'description':
                    description = child.text if child is not None else None
                elif child.tag == 'link':
                    link = child.text if child is not None else None
                elif child.tag == 'pubDate':
                    date = child.text if child is not None else None
                    temp = child.text
                    temp = temp.strip()
                    loc = temp.find('+')
                    if(temp[loc+2] == ":"):
                        temp = temp.replace('+', '+0')
                    temp = dateparser.parse(
                        temp, settings={'TO_TIMEZONE': 'UTC'})
                    date = str(temp).replace('+00:00', '')
                    temp = date.split(" ")
                    temp1 = temp[0].split("-")
                    temp2 = temp[1].split(":")
                    ts = pd.Timestamp(year=int(temp1[0]), month=int(temp1[1]), day=int(
                        temp1[2]), hour=int(temp2[0]), minute=int(temp2[1]), second=int(temp2[2]), tz='utc')
                    ts = ts.to_julian_date()
                    cts = pd.Timestamp(year=1990, month=1, day=1,
                                       hour=0, minute=0, second=0, tz='utc')
                    cts = cts.now() - pd.Timedelta('1 day')
                    cts = int(cts.to_julian_date())
                    if(ts < cts):
                        flag = 1
                    else:
                        flag = 0
                elif child.tag == 'image':
                    imagesrc = child.text if child is not None else None
            if(flag == 1):
                continue
            sent = sa.classify(title) if title is not None else None
            symbol = se.ExtractSymbol(title)if title is not None else None
            if symbol == '':
                symbol = 'NA'
            print(symbol)
            if(flag == 0):
                out_df = out_df.append(pd.Series(
                    [title, description, source, link, date, imagesrc, sent, symbol, category], index=df_cols), ignore_index=True)

    mydb = mysql.connector.connect(host="192.168.2.89",
                                   user="uatmysql",
                                   passwd="uatmysql",
                                   database="PythonNews")

    mycursor = mydb.cursor()
<<<<<<< HEAD
<<<<<<< HEAD

    sql = "INSERT INTO news2(hash_id,title,description,source,link,Newsdate,imagesrc,sent,symbol,category) VALUES( %s,%s,%s, %s,%s,%s,%s,%s,%s,%s)"
=======
    sql = "INSERT INTO news(hash_id,title,description,source,link,Newsdate,insertion_date,imagesrc,sent,symbol,category) VALUES( %s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s)"
>>>>>>> 72fe77d2272513ea11e67ae347a712ee4e95678e
=======
    sql = "INSERT INTO news(hash_id,title,description,source,link,Newsdate,insertion_date,imagesrc,sent,symbol,category) VALUES( %s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s)"
>>>>>>> 72fe77d2272513ea11e67ae347a712ee4e95678e

    for i in range(0, len(out_df)):
        h = hashlib.new('ripemd160')
        title = bytes(str(out_df['title'][i]), 'utf-8')
        h.update(title)
        titleDigest = h.hexdigest()
        if hash_matcher(titleDigest):
<<<<<<< HEAD
<<<<<<< HEAD
            values = (titleDigest, str(out_df['title'][i]), removeTag(str(out_df['description'][i])), str(out_df['source'][i]), str(out_df['link'][i]),
                      str(out_df['date'][i]), str(out_df['imagesrc'][i]), str(out_df['sent'][i]), str(out_df['symbol'][i]), str(out_df['category'][i]))

=======
            values = (titleDigest, str(out_df['title'][i]), removeTag(str(out_df['description'][i]).replace("#39", ",")), str(out_df['source'][i]), str(out_df['link'][i]),
                      str(out_df['date'][i]), getCurrentDateTime(), str(out_df['imagesrc'][i]), str(out_df['sent'][i]), str(out_df['symbol'][i]), str(out_df['category'][i]))
>>>>>>> 72fe77d2272513ea11e67ae347a712ee4e95678e
=======
            values = (titleDigest, str(out_df['title'][i]), removeTag(str(out_df['description'][i]).replace("#39", ",")), str(out_df['source'][i]), str(out_df['link'][i]),
                      str(out_df['date'][i]), getCurrentDateTime(), str(out_df['imagesrc'][i]), str(out_df['sent'][i]), str(out_df['symbol'][i]), str(out_df['category'][i]))
>>>>>>> 72fe77d2272513ea11e67ae347a712ee4e95678e
            mycursor.execute(sql, values)
            mydb.commit()
            Att.attention(str(out_df['symbol'][i]), str(out_df['sent'][i]))
            count += 1
<<<<<<< HEAD
<<<<<<< HEAD
            ClubbSimilar(str(out_df['title'][i]), titleDigest)
=======
=======
>>>>>>> 72fe77d2272513ea11e67ae347a712ee4e95678e
            for j in out_df['date']:
                date, time = str(j).split()
                y, month, d = date.split('-')
                h, m, s = time.split(':')
                ts = pd.Timestamp(year=int(y), month=int(month), day=int(d), hour=int(h), minute=int(m), second=int(s),
                                  tz='utc')
                ts = int(ts.to_julian_date())
                cts = pd.Timestamp(year=1990, month=1, day=1, hour=0, minute=0, second=0, tz='utc')
                cts = cts.now() - pd.Timedelta('1 day')
                cts = int(cts.to_julian_date())
                #print(int(ts)," > ",cts)
                if (int(ts) >= cts):
                    #print('inside')
                    DataLoader()
                    ClubbSimilar(str(out_df['title'][i]), titleDigest)
<<<<<<< HEAD
>>>>>>> 72fe77d2272513ea11e67ae347a712ee4e95678e
=======
>>>>>>> 72fe77d2272513ea11e67ae347a712ee4e95678e
    print(count, "record inserted.")

# print(out_df)
# print(time.clock() - start_time, "seconds")
