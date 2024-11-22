### SQL_Class.py
### Author: Andrew Larkin
### Date Created: May 17, 2022
### Summary: Class for querying training records SQL database

import psycopg2
import pandas as ps
import pandas.io.sql as psql

class SQL_DB:

    # create an instance of the SQL database class
    # INPUTS:
    #    dbDict (dictionary) - contains key value pairs for SQL login parameters
    #    debug (boolean) - whether to connect to a debug (local) or deployment (remote) database
    def __init__(self, dbDict,debug=False):
        self.setupConnection(dbDict,debug)

    # connect to the SQL dabase and return driver for queries
    # INPUTS:
    #    dbDict (dictionary) - contains key value pairs for SQL login parameters
    #    debug (boolean) - whether to connect to a debug (local) or deployment (remote) database
    def setupConnection(self,dbDict,debug):

        # connect to local database for testing and debugging
        if(debug==True):
            print("enterting debug mode")
            conn = psycopg2.connect("dbname=" + dbDict['db'] + " user=" + dbDict['user'] + " password=" + dbDict['pw'])
            self.conn = conn
            self.testSQLConnection()
            
        # connect to remote deployment database
        else:
            print("entering deployment mode")
            conn = psycopg2.connect("host= " + dbDict['host'] + " dbname=" + dbDict['db'] + " user=" + dbDict['user'] + " password=" + dbDict['pw'])
            self.conn = conn
            self.testSQLConnection()

    # determine if the instance has a sucessful SQL connection
    def testSQLConnection(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT version();")
            # Fetch result
            record = cur.fetchone()
            print("You are connected to - ", record, "\n")
            
    # insert post into twitter_records table
    # INPUTS:
    #    recordDict (pandas dataframe) - twitter record to insert
    #    imgId (int) - unique identifier for each twitter record. Name is not ideal, should have called it record id
    def insertTwitterRecord(self,recordDict,imgId):
        query = """INSERT INTO twitter_records(img_id,text,img_http,keyword,category) VALUES (%s,%s,%s,%s,%s);"""
        try:
            with self.conn.cursor() as cur:
                cur.execute(query,
                    (
                        str(imgId),
                        recordDict['text'],
                        recordDict['img_name'],
                        recordDict['kw'],
                        recordDict['cat']
                    )
                )
                self.conn.commit()
                cur.close()
        except Exception as e:
            # insertion was unsucessful revert back to state of previous commit
            self.conn.rollback()
            print(e)
            
    # given a csv containing twitter records, insert records into the twitter_records table
    # INPUTS:
    #    trainingCSV (str) - absolute filepath where twitter records csv is stored
    def insertTrainingRecordsFromCSV(self,trainingCSV):

        # read csv file and randomly shuffle the dataset
        trainingRecords = ps.read_csv(trainingCSV)
        trainingRecords = trainingRecords.sample(frac=1)

        # for each twitter record, insert the record into the twitter_records table along with the record index
        nRecords = trainingRecords.count()[0]
        maxId = int(self.getCurMaxId())
        curId = maxId+1
        for rowIndex in range(nRecords):
            curRow = trainingRecords.iloc[rowIndex]
            self.insertTwitterRecord(curRow,curId)
            curId+=1
    
    # get the highest record index currently in the twitter_records table
    # OUTPUTS:
    #    maximum index (integer)
    def getCurMaxId(self):
        query = """SELECT MAX(cast(img_id as integer)) from twitter_records;"""
        try: 
            with self.conn.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()
                return(int(row[0]))
        except Exception as e:
            print(str(e))

    # retrive tweets a given worker coded during a given time interval
    # INPUTS:
    #    userId (str) - unique user id
    #    startDay (int) - day of the month for start of time range (inclusive)
    #    endDay (int) - day of the month for end of time range (inclusive)
    #    startMonth (int) - month of the year for start of time range (inclusive)
    #    endMonth (int) - month of the year for end of time range (inclusive)
    #    year (int) - year for time range (time range cannot overlap multiple years)
    #    tableName (str) - name of the table to retreive records from
    # OUTPUTS:
    #    df (pandas dataframe) - contains tweets coded by worker betweeen startdate and enddate
    def getTweetsOneUser(self,userId,startDay,startMonth,endDay,endMonth,year,tableName):
        startStamp = str(year) + "-" + str(startMonth) + "-" + str(startDay)
        endStamp = str(year) + "-" + str(endMonth) + "-" + str(endDay)
        try:
            query="""SELECT *from """ + tableName + """ where user_id = '""" + str(userId) + """' and """
            query2 = """time_submitted >= '""" + startStamp + """' and """
            query3 = """time_submitted <= '""" + endStamp + """'"""
            df = psql.read_sql(query+query2+query3,self.conn)
            return df

        except Exception as e:
            print(str(e))

    # retreive all users who labeled tweets for a category (place, child, health)
    # within a given time interval
    # INPUTS:
    #    startDay (int) - day of the month for start of time range (inclusive)
    #    endDay (int) - day of the month for end of time range (inclusive)
    #    startMonth (int) - month of the year for start of time range (inclusive)
    #    endMonth (int) - month of the year for end of time range (inclusive)
    #    year (int) - year for time range (time range cannot overlap multiple years)
    #    tableName (str) - name of the table to retreive records from
    # OUTPUTS:
    #    records (str array) - list of user names who coded tweets for the given catergory
    def getUserIdsInTime(self,startDay,startMonth,endDay,endMonth,year,tableName):
        startStamp = str(year) + "-" + str(startMonth) + "-" + str(startDay)
        endStamp = str(year) + "-" + str(endMonth) + "-" + str(endDay)
        query="""SELECT distinct(user_id) from """ + tableName + """ where """
        query2 = """time_submitted >= '""" + startStamp + """' and """
        query3 = """time_submitted <= '""" + endStamp + """' order by user_id"""
        records = []
        try:
            with self.conn.cursor() as cur:
                cur.execute(query+query2+query3)
                for row in cur.fetchall():
                    records.append(row[0])
                return(records)
        except Exception as e:
            print(str(e))

    # get the number of tweets each user labeled for a specific category (place, child, health) during a 
    # specific time interval
    # INPUTS:
    #    startDay (int) - day of the month for start of time range (inclusive)
    #    endDay (int) - day of the month for end of time range (inclusive)
    #    startMonth (int) - month of the year for start of time range (inclusive)
    #    endMonth (int) - month of the year for end of time range (inclusive)
    #    year (int) - year for time range (time range cannot overlap multiple years)
    #    tableName (str) - name of the table to retreive records from
    # OUTPUTS:
    #    number of tweets for each user in a pandas dataframe
    def getUserTweetsInTime(self,startDay,startMonth,endDay,endMonth,year,tableName):
        startStamp = str(year) + "-" + str(startMonth) + "-" + str(startDay)
        endStamp = str(year) + "-" + str(endMonth) + "-" + str(endDay)
        try:
            query="""SELECT count(user_id) as n_tweets,user_id from (select * from """ + tableName + """ where """
            query2 = """time_submitted >= '""" + startStamp + """' and """
            query3 = """time_submitted <= '""" + endStamp + """'"""
            query4 = """) as a group by user_id order by user_id"""
            df = psql.read_sql(query+query2+query3+query4,self.conn)
            return(df)
        except Exception as e:
            print(str(e))

    # select 10 random labeled tweets from one worker
    # INPUTS:
    #    userId (str) - unique worker id
    #    tableName (str)- table containing labeled tweets
    # OUTPUTS:
    #    dataframe containing 10 randomly selected tweets and accompanying labels
    def selectRandomForUser(self,userId,tableName):
        query="""SELECT user_id,img_id,location_cat,location,is_child,age,"""
        query2 = """is_health, health_impact, health_type from """ + tableName + """  where user_id = '"""
        query3 = userId + """' ORDER BY random() limit 10;"""
        try:
            df = psql.read_sql(query+query2+query3,self.conn)
            return(df)
        except Exception as e:
            print(str(e))

    def origTwitterRecords(self):
        query="""SELECT * from twitter_records; """
        df = psql.read_sql(query,self.conn)
        return(df)
    
    def getChildTwitterRecords(self):
        childT1 = psql.read_sql("""SELECT * from child_tweets; """,self.conn)
        childT2 = psql.read_sql("""SELECT * from child_tweets2; """,self.conn)
        childT3 = psql.read_sql("""SELECT * from child_tweets3; """,self.conn)
        return(ps.concat([childT1,childT2,childT3]))
    
    def getPlaceTwitterRecords(self):
        placeT1 = psql.read_sql("""SELECT * from place_tweets; """,self.conn)
        placeT2 = psql.read_sql("""SELECT * from place_tweets2; """,self.conn)
        placeT3 = psql.read_sql("""SELECT * from place_tweets3; """,self.conn)
        return(ps.concat([placeT1,placeT2,placeT3]))

    def getHealthTwitterRecords(self):
        healthT1 = psql.read_sql("""SELECT * from health_tweets; """,self.conn)
        healthT2 = psql.read_sql("""SELECT * from health_tweets2; """,self.conn)
        healthT3 = psql.read_sql("""SELECT * from health_tweets3; """,self.conn)
        return(ps.concat([healthT1,healthT2,healthT3]))
    
    def getTweetLabels(self):
        labels = psql.read_sql("""SELECT * from twitter_labels; """,self.conn)
        return(labels)
    


# runtime commands used to populate remote SQL database

#a = SQL_DB(dictDeploy,False)
#a.insertTrainingRecordsFromCSV('insert csv filepath here')