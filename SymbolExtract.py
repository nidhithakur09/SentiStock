import pandas as pd
import re
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import mysql.connector

SymData = pd.read_csv("SymbolNameList.csv", sep=",",
                      header=None, names=['Sym', 'Name'])
Symbols = SymData["Sym"].tolist()
Names = SymData["Name"].tolist()

f2 = open("Exceptions.txt", "r")
excep = f2.read().split("\n")


def ExtractSymbol(title):
    final = ""
    df_matchedSym = []
    matchedSymList = ""
    for i in range(0, len(Symbols)):
        sym = Symbols[i].lower()
        st = title.lower()
        if(Symbols[i] in excep):
            st = title
            sym = Symbols[i]
        temp = []
        reg = r"\b"+sym+r"\b"
        reg = re.compile(reg)
        if(reg.search(st)):
            temp.append(Symbols[i])
            temp.append(Names[i])
            temp.append(True)
        else:
            temp.append(Symbols[i])
            temp.append(Names[i])
            temp.append(False)
        df_matchedSym.append(temp)
    df_matchedSym = [z for z in df_matchedSym if z[2] == True]
    if df_matchedSym:
        for term in df_matchedSym:
            matchedSymList = matchedSymList+"|"+term[0]
    df_matchedSym = []
    temp2 = re.sub('[^A-Za-z0-9. ]+', '', title)
    for i in range(0, len(Names)):
        thres=50
        tsor = fuzz.token_sort_ratio(temp2.lower(), Names[i].lower())
        tser = fuzz.token_set_ratio(temp2.lower(), Names[i].lower())
        r = fuzz.ratio(temp2.lower(), Names[i].lower())
        pr = fuzz.partial_ratio(temp2.lower(), Names[i].lower())
        avg = (tsor+tser+r+pr)/4
        if(Names[i]=="CONSOLIDATED CONSTRUCTION"):
            thres = 60
        temp = []
        if(avg >= thres):
            temp.append(Symbols[i])
            temp.append(Names[i])
            temp.append(True)
        else:
            temp.append(Symbols[i])
            temp.append(Names[i])
            temp.append(False)
        df_matchedSym.append(temp)
    
    df_matchedSym = [z for z in df_matchedSym if z[2] == True]
    if(len(df_matchedSym) == 0):
        if(matchedSymList == ""):
            final = ""
        else:
            final = final+matchedSymList
    else:
        for i in range(0, len(df_matchedSym)):
            reg = r"\b"+df_matchedSym[i][0].lower()+r"\b"
            reg = re.compile(reg)
            if(reg.search(matchedSymList.lower())):
                continue
            symSplit = df_matchedSym[i][1].split(" ")
            reg = r"\b"+symSplit[0].lower()+r"\b"
            reg = re.compile(reg)
            if(reg.search(temp2.lower())):
                if(len(symSplit) < 3):
                    matchedSymList = matchedSymList+"|"+df_matchedSym[i][0]
                else:
                    
                    reg = r"\b"+symSplit[1].lower()
                    reg = re.compile(reg)
                    if(reg.search(temp2.lower())):
                        matchedSymList = matchedSymList+"|"+df_matchedSym[i][0]
        final = final+matchedSymList
    return final

# def db_rect():
#     mydb = mysql.connector.connect(host="192.168.2.89",
#                                    user="uatmysql",
#                                    passwd="uatmysql",
#                                    database="NewsField")
#     mycursor = mydb.cursor()
#     query = "select rn_news_id,rn_title from Rupee_News"
#     mycursor.execute(query)
#     records = mycursor.fetchall()
#     tot = len(records)
#     i=1
#     for row in records:
#         print(i,"/",tot)
#         symbol = ExtractSymbol(row[1])
#         if symbol == "":
#             symbol="NA"
#         query = "update Rupee_News set rn_symbol=%s where rn_news_id=%s"
#         data=(symbol,row[0])
#         mycursor.execute(query,data)
#         i+=1
#     mydb.commit()
# db_rect()

