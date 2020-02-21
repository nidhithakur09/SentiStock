import mysql.connector
import NewsCategory as nc
mydb = mysql.connector.connect(host="192.168.2.89",
                                   user="uatmysql",
                                   passwd="uatmysql",
                                   database="NewsField")
mycursor = mydb.cursor()
query = "select rn_news_id,rn_title from Rupee_News_All"
mycursor.execute(query)
records = mycursor.fetchall()
tot = len(records)
i=1
for row in records:
    print(i,"/",tot)
    cat = nc.categorize(row[1])
    query = "update Rupee_News_All set rn_topic=%s where rn_news_id=%s"
    data=(cat,row[0])
    mycursor.execute(query,data)
    i+=1
mydb.commit()