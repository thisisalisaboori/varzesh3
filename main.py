from bs4 import BeautifulSoup
import bs4
import requests as rq
import psycopg2
import os
PG_HOST=os.environ.get("PG_HOST","localhost")
PG_PORT=os.environ.get("PG_PORT","5432")
PG_PASS=os.environ.get("PG_PASS","123@123")
PG_USER=os.environ.get("PG_USER","ali")

LEAGUEE={'1':'بوندس لیگا','2':'لالیگا','3':'لیگ برتر انگلیس','4':'سری آ' , '5':'لیگ یک فرانسه','6':'لیگ برتر ایران','55':'لیگ 1 پرتغال' }
IRAN=['','1401-1400','1400-1399','1399-1398','1398-1397','1397-1396','1396-1395','1395-1394','1394-1393','1393-1392',
      '1392-1391','1391-1390','1390-1389','1389-1388','1388-1387','1387-1386','1386-1385','1385-1384','1384-1383','1383-1382'
      '1381-1380']

OTHER=['','2022-2021','2021-2020','2020-2019','2019-2018','2018-2017','2017-2016','2016-2015','2015-2014','2014-2013','2013-2012'
       ,'2012-2011','2011-2010','2010-2009','2009-2008']

class ILeague:
    
    def __init__(self,index) -> None:
        self.index= index
        self.URL=f"https://www.varzesh3.com/football/league/{self.index}/"
        self.conn = psycopg2.connect(database="Varzesh3", user = PG_USER, password = PG_PASS, host = PG_HOST, port = "5432")
    def GetLeagueResult(self,index,priod):
        pass
    
    def crawler(self,p):
        
        if p =='':
            url=self.URL
        else:
            url= f"{self.URL}{p}/" 
        print(url)
        response=rq.get(url)
        
        cur = self.conn.cursor()  
        q= f"""select "Id" from "Countries" where "Varzesh3Id" = {self.index} limit 1"""
        cur.execute(q)
        ID=0
        for row in cur :
            ID=row[0]

        IDLEAGUE=0
        while  IDLEAGUE ==0:
            cur.execute(f"""select "Id" from "LeagueHistories" where "CountryId" = {ID} and "Period" = '{p}' limit 1 """)
            for row in cur :
                IDLEAGUE=row[0]
            if IDLEAGUE ==0 :
                cur.execute(f""" insert into "LeagueHistories"("Period", "CountryId") values('{p}',{ID} ) """)
                self.conn.commit()
            print(p,IDLEAGUE)
        #return 
        bs=bs4.BeautifulSoup( response.text,'html.parser')
        x=bs.find(class_='league-standing')
        if x is None:
            print(url)
            return
        rows=x.find_all('tr')
        for tr in rows:
            cols=tr.find_all('td')
            if cols is None or len(cols) == 0  :
                continue
            rank= int(cols[0].text.strip())
            team= cols[1].text.strip()
            game = int(cols[2].text.strip())
            win =int(cols[3].text.strip())
            eq= int(cols[4].text.strip())
            loss=int(cols[5].text.strip())
            av=int(cols[7].text.strip())
            point =int(cols[8].text.strip())
            #print(team,game,win,eq,loss,av,point)
            cur = self.conn.cursor()         
            cur.execute(f"""insert into "LeagueDetails"("LeagueId","Rank","Team","Play","Win","Eq","Loss","Av","Point") 
                        values({IDLEAGUE},{rank},'{team}',{game},{win},{eq},{loss},{av},{point})""")
        self.conn.commit()    

class IranLeague(ILeague):
    def GetLeagueResult(self):
        for p in IRAN:
            self.crawler(p)


class League(ILeague):
    
    def GetLeagueResult(self):
        for p in OTHER:
            self.crawler(p)

def StartCrawler():
    for i in LEAGUEE:
        if i == '6':
            league= IranLeague(i)
        else:
            league= League(i)
        league.GetLeagueResult()
        
        
StartCrawler()