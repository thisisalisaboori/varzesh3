from bs4 import BeautifulSoup
import bs4
import requests as rq
import psycopg2
import os
PG_HOST=os.environ.get("PG_HOST","localhost")
PG_PORT=os.environ.get("PG_PORT","5432")
PG_PASS=os.environ.get("PG_PASS","123@123")
PG_USER=os.environ.get("PG_USER","ali")

LEAGUEE={'1':'بوندسلیگا-آلمان','2':'لالیگا-اسپانیا','3':'لیگ-برتر-انگلیس','4':'سری-آ-ایتالیا' ,
         '5':'لیگ-یک-فرانسه','6':'لیگ-برتر-ایران','55':'لیگ-برتر-پرتغال' }
IRAN=['','1400-1401','1399-1400','1398-1399','1397-1398','1396-1397','1395-1396','1394-1395','1393-1394','1392-1393',
      '1391-1392','1390-1391','1389-1390','1388-1389','1387-1388','1386-1387','1385-1386',
      '1384-1385','1383-1384','1382-1383'
      '1382-1381']

OTHER=['','2021-2022','2020-2021','2019-2020','2018-2019','2017-2018','2016-2017','2015-2016','2014-2015','2013-2014'
       ,'2012-2013'
       ,'2011-2012','2010-2011','2009-2010','2008-2009']

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
            #https://www.varzesh3.com/football/league/1/%D8%A8%D9%88%D9%86%D8%AF%D8%B3%D9%84%DB%8C%DA%AF%D8%A7-%D8%A7%D9%93%D9%84%D9%85%D8%A7%D9%86/2021-2022
            title= LEAGUEE.get(self.index)
            url= f"{self.URL}{title}/{p}" 
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