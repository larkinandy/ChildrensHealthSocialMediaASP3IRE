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