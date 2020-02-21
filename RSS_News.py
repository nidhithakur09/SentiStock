from bs4 import BeautifulSoup
import requests
import xml.etree.ElementTree as ET
import re
import mysql.connector
import time
import hashlib
import SentimentAnalyser as sa
import SymbolExtract as se
import dateparser
from DataClubbing import *
import unicodedata
import pandas as pd
from datetime import datetime
# import Attention as Att
# import pysolr
# import NewsCategory as nc
import lxml.html

# solr = pysolr.Solr('http://localhost:8983/solr/Rupee_News', timeout=10)
print("Start  : ", time.ctime())
info = pd.read_csv("urls.csv")
url = info['Link']
source = info['Source']

start_time = time.time()

c_time = datetime.now().time()
today8am = c_time.replace(hour=8, minute=0, second=0, microsecond=0)
today8pm = c_time.replace(hour=20, minute=0, second=0, microsecond=0)
# if c_time < today8am:
#     Att.update()
today8amu = c_time.replace(hour=2, minute=30, second=0, microsecond=0)
today8pmu = c_time.replace(hour=14, minute=30, second=0, microsecond=0)


def getCurrentDateTime():
    from datetime import datetime
    now = datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S")


def removeTag(title):
    string = re.sub(r'(#[0-9][0-9])', r'&\1', title.strip())
    string = lxml.html.fromstring(string)
    string  = lxml.html.tostring(string).decode("utf-8")
    #to remove html tags
    string = re.sub(r'(<[A-Za-z\/][^>]*>)', '', string)

    # remove all single characters
    string = re.sub(r'\s+[a-zA-Z]\s+', ' ',string)

    # Substituting multiple spaces with single space
    string = re.sub(r' +', ' ',string)

    # Removing prefixed 'b'
    string = re.sub(r'^b\s+', '', string)
    string = re.sub(r'&amp;','&',string)
    string = re.sub(r'&#(\d+);',lambda m: chr(int(m.group(1))),string)
    return string


def html_parser_title(title_link):
    url_get = requests.get(title_link)
    soup = BeautifulSoup(url_get.content, 'lxml')
    soup.find_all('div', {'class': 'brk_wraper clearfix'})
    return soup.select('h1')[0].text


def html_parser_description(title_link):
    url_get = requests.get(title_link)
    soup = BeautifulSoup(url_get.content, 'lxml')
    soup.find_all('div', {'class': 'brk_wraper clearfix'})
    return soup.select('h2')[0].text


def match_count():
    mydb = mysql.connector.connect(host="localhost",
                                   user="root",
                                   passwd="",
                                   database="SentiStock")

    mycursor = mydb.cursor()
    query = 'update SentiStock.SS_News set ss_match_count = (select count(SS_News.ss_news_id) from SentiStock.SS_Similar_News where SS_Similar_News.ss_parent_id = SS_News.ss_news_id) where ss_news_id = ss_news_id; '
    mycursor.execute(query)
    mycursor.commit()


def hash_matcher(ss_news_id):
    mydb = mysql.connector.connect(host="localhost",
                                   user="root",
                                   passwd="",
                                   database="SentiStock")

    mycursor = mydb.cursor()

    hash_string = "SELECT ss_news_id FROM SS_News WHERE ss_news_id = '{}';".format(ss_news_id)
    mycursor.execute(hash_string)
    records = mycursor.fetchall()
    if len(records) >= 1:
        return False
    else:
        return True


count = 0
print("total No of Newsfeeds:", len(url))

for i in range(0, len(url)):
    print(url[i])
    title = ''
    description = ''
    imagesrc = ''
    link = ''
    date = ''
    df_cols = ['titleDigest', 'title', 'description', 'source', 'link',
               'date', 'imagesrc', 'category']
    out_df = pd.DataFrame(columns=df_cols)
    print('url count:', i)
    count = 0
    flag = 0
    filename = str("economic.xml")
    response = requests.get(url[i])
    try:
        with open(filename, 'wb') as f:
            f.write(response.content)
        tree = ET.parse(filename)
        pass
    except:
        print("An error occured with url ", url[i])
    else:
        root = tree.getroot()
        newsdata = []
        for item in root.findall('./channel/item'):
            title = ''
            description = ''
            imagesrc = ''
            link = ''
            date = ''
            # empty news dictionary
            category = info['Category'][i]
            source = info['Source'][i]
            for child in item:
                if child.tag == 'title':
                    title = child.text
                elif child.tag == 'description':
                    description = child.text
                elif child.tag == 'link':
                    link = child.text
                    if ".html" in link:
                        try:
                            title = html_parser_title(url[i])
                            description = html_parser_description(url[i])
                        except:
                            pass
                elif child.tag == 'pubDate':
                    date = child.text
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
                    imagesrc = child.text
            if(flag == 1):
                break
            if imagesrc == '':
                imagesrc = 'NA'
            if description == '' or description == 'NULL':
                description = 'NA'

            title = unicodedata.normalize(
                'NFKD', title).encode('utf-8', 'replace')
            title = title.decode('utf-8', 'replace')
            if description == None or str(removeTag(description)) == ' NULL' or str(removeTag(description)) == 'NOT FOUND':
                description = 'NA'
            else:
                description = unicodedata.normalize(
                    'NFKD', description).encode('utf-8', 'replace')
                description = description.decode('utf-8', 'replace')
            title = title.strip()
            if(title == ""):
                continue
            if (title == 'NA' and description == 'NA') or (title == None and description == None):
                continue

            title = str(title).replace("#39", "'").strip()
            h = hashlib.new('ripemd160')
            htitle = bytes(str(title), 'utf-8')
            h.update(htitle)
            titleDigest = h.hexdigest()

            if hash_matcher(titleDigest):
                out_df = out_df.append(pd.Series(
                    [titleDigest, removeTag(title), removeTag(description), source, link, date, imagesrc, category], index=df_cols), ignore_index=True)
            else:
                break

    try:
        mydb = mysql.connector.connect(host="localhost",
                                        user="root",
                                        passwd="",
                                        database="SentiStock")

        mycursor = mydb.cursor()
        sql = "INSERT INTO SS_News(ss_news_id,ss_source, ss_link, ss_time,ss_entry_time, ss_title, ss_image_link, ss_description, ss_sentiments, ss_symbol, ss_category,ss_full_description) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        for i in range(0, len(out_df)):
            sent = 'NA'
            symbol = 'NA'
            description = str(out_df['description'][i]).replace("#39", "'").strip()
            if out_df['title'][i] != None and out_df['title'][i] != ' ':
                sent = sa.classify(out_df['title'][i])
                symbol = se.ExtractSymbol(out_df['title'][i])
            if symbol == '':
                symbol = 'NA'
            values = (str(out_df['titleDigest'][i]), str(out_df['source'][i]), str(out_df['link'][i]), str(out_df['date'][i]), getCurrentDateTime(
            ), out_df['title'][i], str(out_df['imagesrc'][i]), str(out_df['description'][i]), sent, symbol, str(out_df['category'][i]),str(out_df['description'][i]))
            mycursor.execute(sql, values)
            mydb.commit()
            count += 1
            print(count, "record inserted.")
            # ClubbSimilar(out_df['title'][i], out_df['titleDigest'][i])
            
    except Exception as e:
        mydb.rollback()

        print(str(out_df['titleDigest'][i]))
    else:
        pass

        # ptime = datetime.strptime(str(out_df['date'][i]), '%Y-%m-%d %H:%M:%S')
        # ptime = ptime.time()
        # if(ptime >= today8amu and ptime <= today8pmu):
        #     Att.attention(symbol, sent)


    out_df = out_df.drop(out_df.index, inplace=True)
print("End  : ", time.ctime())
