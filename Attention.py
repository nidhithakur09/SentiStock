import mysql.connector
import pandas as pd
from statistics import mean, stdev


def update():
    mydb = mysql.connector.connect(host="localhost",
                                   user="root",
                                   passwd="12",
                                   database="SentiStock")
    mycursor = mydb.cursor()
    query = "UPDATE SS_News_Attention SET ss_sentiment=0,ss_total_count=ss_total_count+1, ss_sum=ss_sum+ss_c_count, ss_square_sum=ss_square_sum+POW(ss_c_count,2), ss_c_count=0, ss_zscore=0 WHERE ss_c_count>0"
    mycursor.execute(query)
    mydb.commit()



def attention(symbol, sent):
    mydb = mysql.connector.connect(host="localhost",
                                   user="root",
                                   passwd="12",
                                   database="SentiStock")
    mycursor = mydb.cursor()
    att_sym = symbol.split('|')
    att_sym = list(filter(None, att_sym))
    if len(att_sym) > 0 and att_sym[0] != 'NA':
        for sym in att_sym:
            sentv = 0
            if(sent == "Negative"):
                sentv = -1
            elif(sent == "Positive"):
                sentv = 1

            symb = (sym,)
            sql = "SELECT ss_total_count, ss_sum, ss_square_sum, ss_c_count FROM SS_News_Attention WHERE ss_symbol = %s"
            mycursor.execute(sql, symb)
            results = mycursor.fetchall()
            if len(results) != 0:

                total_count = results[0][0]
                h_sum = results[0][1]
                square_sum = results[0][2]
                c_count = results[0][3]+1
            else:
                sql = "Insert into SS_News_Attention(ss_symbol) Values(%s)"
                mycursor.execute(sql, symb)
                total_count = 0
                h_sum = 0
                square_sum = 0
                c_count = 1

            zscore = 0
            if(total_count == 0):
                zscore = c_count
            else:
                avg = h_sum/total_count
                sqr_avg = square_sum/total_count
                std = sqr_avg - avg**2
                if std == 0:
                    temp = c_count - avg
                    if(temp != 0):
                        zscore = temp
                    else:
                        zscore = 0
                else:
                    zscore = (c_count - avg)/std
            sql = "UPDATE SS_News_Attention SET ss_c_count=ss_c_count+1, ss_sentiment=ss_sentiment+ %s , ss_zscore = %s where ss_symbol = %s"
            upd = (sentv, str(round(zscore, 2)), sym)
            mycursor.execute(sql, upd)
            mydb.commit()
    mydb.close()


def rect():
    mydb = mysql.connector.connect(host="localhost",
                                   user="root",
                                   passwd="12",
                                   database="SentiStock")
    mycursor = mydb.cursor()
    sql = "select ss_sentiments, ss_symbol from SentiStock.SS_News where ss_time>='2019-07-30'"
    mycursor.execute(sql)
    results=mycursor.fetchall()
    tot = len(results)
    i=0
    for result in results:
        attention(result[1],result[0])
        print(i,"/",tot)
        i+=1

rect()

