#!/usr/bin/env python
# coding: utf-8

############ FlaskBackend.py #############
### Author: Andrew Larkin 
### Created for the ASP3IRE Social Media Project 
### Summary: 
###   Provide a backend server for retreiving social media posts that need to be labeled
###   from a PostgreSQL database, and storing updated social media posts labeled by students 

### environment:
### written for and tested in Heroku, using 1 professional level dyno and 1 hobby PostgreSQL database
### occasionally crashed when request rate exceeded the databaase ability to retreive results (around 5 requests/sec)

# import libraries 
import os
from werkzeug.wrappers import Request, Response
from flask import Flask, render_template, request, url_for, jsonify
from flask_cors import CORS
import psycopg2 
import sqlalchemy
import random # for random probability of sampling a QA tweet

# vars set by Heroku
DATABASE_URL = os.environ['DATABASE_URL']

# connect to SQL database
conn = psycopg2.connect(DATABASE_URL,sslmode="require")
conn.autocommit = True
app = Flask(__name__)
CORS(app)

# get list of workers who are certified to code tweets. 
# only these workers will get a tweet from the 'code' GET parameter
# OUTPUTS:
#    validatedWorkers (str array) - list of validated worker user ids
def getValidatedWorkers():
    validatedWorkers = []
    try:
        with conn.cursor() as cur:
            query = """SELECT user_id from participants where status = 'certified'""" 
            cur.execute(query)
            results = cur.fetchall()
            for row in results:
                validatedWorkers.append(row[0])
            return(validatedWorkers)
    except Exception as e:
        print(str(e))


# get a record from SQL database.  Originally used for labeling and practice, now only used for practice tweets
# INPUTS:
#    recordNum (int) - unique tweet number
# OUTPUTS:
#    record (dict) - tweet image info to sent to client
def sampleRecordFromSQL(recordNum):
    try:
        recordNum = max(recordNum,0)
        with conn.cursor() as cur:
            query = """SELECT * from twitter_records where img_id = '""" + str(recordNum) + """'"""
            cur.execute(query)
            row = cur.fetchone()
            # variables in sample dict are needed to load a tweet into the client coding webpage
            sampleDict = {
                'imgId':row[0],
                'text':row[1],
                'imgHttp':row[2],
                'qa':'none'
            }
            return(sampleDict)
    except Exception as e:
        print(str(e))


# get a record that needs to be labeled for places from the SQL database.  
# can be called for labeling only
# INPUTS:
#    recordNum (int) - unique tweet number
# OUTPUTS:
#    record (dict) - tweet image info to sent to client
def samplePlaceRecordFromSQL(recordNum):
    try:
        recordNum = max(recordNum,0)
        with conn.cursor() as cur:
            query = """SELECT * from place_tweets where id = '""" + str(recordNum) + """'"""
            cur.execute(query)
            row = cur.fetchone()
            # variables in sample dict are needed to load a tweet into the client coding webpage
            sampleDict = {
                'imgId':row[0],
                'text':row[1],
                'imgHttp':row[2],
                'qa':'none'
            }
            return(sampleDict)
    except Exception as e:
        print(str(e))


# get a record from the tables storing the third batch of training records that need to be labeled for place.  
# can be called for coding only
# INPUTS:
#    recordNum (int) - unique tweet number
# OUTPUTS:
#    record (dict) - tweet image info to sent to client
def samplePlaceRecordFromSQL2(recordNum):
    try:
        recordNum = max(recordNum,0)
        with conn.cursor() as cur:
            query = """SELECT * from place_tweets3 where index = '""" + str(recordNum) + """'"""
            cur.execute(query)
            row = cur.fetchone()
            # variables in sample dict are needed to load a tweet into the client coding webpage
            sampleDict = {
                'imgId':row[0],
                'text':row[2],
                'imgHttp':row[1],
                'qa':'none'
            }
            return(sampleDict)
    except Exception as e:
        print(str(e))

# get a record that needs to be labeled for child development stages from the SQL database.  
# can be called for labeling only
# INPUTS:
#    recordNum (int) - unique tweet number
# OUTPUTS:
#    record (dict) - tweet image info to sent to client
def sampleChildRecordFromSQL(recordNum):
    try:
        recordNum = max(recordNum,0)
        with conn.cursor() as cur:
            query = """SELECT * from child_tweets where id = '""" + str(recordNum) + """'"""
            cur.execute(query)
            row = cur.fetchone()
            # variables in sample dict are needed to load a tweet into the client coding webpage
            sampleDict = {
                'imgId':row[0],
                'text':row[1],
                'imgHttp':row[2],
                'qa':'none'
            }
            return(sampleDict)
    except Exception as e:
        print(str(e))

# get a record from the tables storing the third batch of training records that need to be labeled
# for child development stages. Can be called for coding only
# INPUTS:
#    recordNum (int) - unique tweet number
# OUTPUTS:
#    record (dict) - tweet image info to sent to client
def sampleChildRecordFromSQL2(recordNum):
    try:
        recordNum = max(recordNum,0)
        with conn.cursor() as cur:
            query = """SELECT * from child_tweets3 where index = '""" + str(recordNum) + """'"""
            cur.execute(query)
            row = cur.fetchone()
            # variables in sample dict are needed to load a tweet into the client coding webpage
            sampleDict = {
                'imgId':row[0],
                'text':row[2],
                'imgHttp':row[1],
                'qa':'none'
            }
            return(sampleDict)
    except Exception as e:
        print(str(e))

# get a record that needs to be labeled for health impacts from the SQL database.  
# can be called for labeling only
# INPUTS:
#    recordNum (int) - unique tweet number
# OUTPUTS:
#    record (dict) - tweet image info to sent to client
def sampleHealthRecordFromSQL(recordNum):
    try:
        recordNum = max(recordNum,0)
        with conn.cursor() as cur:
            query = """SELECT * from health_tweets where id = '""" + str(recordNum) + """'"""
            cur.execute(query)
            row = cur.fetchone()
            # variables in sample dict are needed to load a tweet into the client coding webpage
            sampleDict = {
                'imgId':row[0],
                'text':row[1],
                'imgHttp':row[2],
                'qa':'none'
            }
            return(sampleDict)
    except Exception as e:
        print(str(e))

# get a record that needs to be labeled for health impacts for the third batch of training records 
# from the SQL database. Can be called for labeling only
# INPUTS:
#    recordNum (int) - unique tweet number
# OUTPUTS:
#    record (dict) - tweet image info to sent to client
def sampleHealthRecordFromSQL2(recordNum):
    try:
        recordNum = max(recordNum,0)
        with conn.cursor() as cur:
            query = """SELECT * from health_tweets3 where index = '""" + str(recordNum) + """'"""
            cur.execute(query)
            row = cur.fetchone()
            # variables in sample dict are needed to load a tweet into the client coding webpage
            sampleDict = {
                'imgId':row[0],
                'text':row[2],
                'imgHttp':row[1],
                'qa':'none'
            }
            return(sampleDict)
    except Exception as e:
        print(str(e))

# tweets are sequentially pulled from the database based on index.  Get the index of the next tweet to label
# OUTPUTS:
#    tweet index (int)
def getCurIndex():
    try:
        with conn.cursor() as cur:
            query = """SELECT max(cur_index) from tweet_index"""
            cur.execute(query)
            row = cur.fetchone()
            return(int(row[0]))
    except Exception as e:
        conn.rollback()
        print(str(e))

# tweets are sequentially pulled from the database based on index.  Get the index of the next tweet to label for 
# the given category (place, child, health)
# INPUTS:
#    category (string) - which type of label to apply to tweet.  Can be place, child, or health
# OUTPUTS:
#     tweet index(int)
def getSampleIndex(category):
    try:
        with conn.cursor() as cur:
            query = """SELECT max(indexVal) from sample_indexes where category='""" + str(category) + """'"""
            cur.execute(query)
            row = cur.fetchone()
            return(int(row[0]))
    except Exception as e:
        conn.rollback()
        print(str(e))

# increment the index for the next tweet to label for the given input category
# INPUTS:
#    newIndex (int) - the new index number
#    category (string) - category of label.  Can be place, child, or health
def updateSampleIndex(newIndex,category):
    try:
        with conn.cursor() as cur:
            query = """INSERT INTO sample_indexes (indexval,category) VALUES (%s,%s) ON CONFLICT (category) DO UPDATE SET indexval = EXCLUDED.indexval;"""
            cur.execute(query,(str(newIndex),str(category)))
            conn.commit()
            cur.close()
    except Exception as e:
        conn.rollback()
        print(str(e))

# increment the index for the next tweet to sample.  Originally used for labeling and practice. 
# now only used for practice tweets
# INPUTS:
#    newIndex (int) - the index of the next tweet to sample
def updateIndex(newIndex):
    try:
        with conn.cursor() as cur:
            query = """INSERT INTO tweet_index (cur_index,counter_for) VALUES (%s,%s) ON CONFLICT (counter_for) DO UPDATE SET cur_index = EXCLUDED.cur_index;"""
            cur.execute(query,(str(newIndex),0))
            conn.commit()
            cur.close()
    except Exception as e:
        conn.rollback()
        print(str(e))

def processQARecord(inputDict):
    try:
        with conn.cursor() as cur:
            query = """INSERT INTO  qa_sample (img_id,location,is_child,user_id,age,is_health,health_impact,health_type,location_cat,qa_type,time_submitted,sample_month)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),to_char(now(), 'mm'))
            ON CONFLICT (img_id,user_id,sample_month) DO UPDATE SET location=EXCLUDED.location, is_child=EXCLUDED.is_child,age=EXCLUDED.age,
            is_health=EXCLUDED.is_health,health_impact=EXCLUDED.health_impact,health_type=EXCLUDED.health_type,location_cat = EXCLUDED.location_cat,
            time_submitted=EXCLUDED.time_submitted
            ;"""
            cur.execute(query,
                (
                    str(inputDict['imgId']),
                    inputDict['location'],
                    inputDict['isChild'],
                    inputDict['userId'],
                    inputDict['age'],
                    inputDict['isHealth'],
                    inputDict['healthImpact'],
                    inputDict['healthType'],
                    inputDict['locationCat'],
                    inputDict['qa']
                )           
            )
            conn.commit()
            cur.close()
    except Exception as e:
        conn.rollback()
        print(str(e))

def sampleQAImage(userId):
    try:
        with conn.cursor() as cur:
            query = """SELECT img_id,text,img_http,qa_type from qa_sample where user_id = '""" + userId
            query2 = """' and sample_month = to_char(now(), 'mm') and time_submitted is NULL""" 
            cur.execute(query + query2)
            row = cur.fetchone()
            if(row==None):
                print("no more qa questions for user %s" %(userId))
                return({})
            sampleDict = {
                'imgId':row[0],
                'text':row[1],
                'imgHttp':row[2],
                'qa':row[3]
            }
            #print("found qa question to sample for user %s" %(userId))
            return(sampleDict)
    except Exception as e:
        print(str(e))
        
def getCatQuery(category):
    catList = []
    if(category=='place'):
        catList = ['home_pos','home_neg','daycare_pos','daycare_neg','park_pos','park_neg','school_pos','school_neg']
    elif(category=='child'):
        catList = ['baby_pos','baby_neg','toddler_pos','toddler_neg','es_pos','es_neg','ms_pos','ms_neg','hs_pos','hs_neg']
    else:
        catList = ['cog_pos','cog_neg','emot_pos','emot_neg','phys_pos','phys_neg']
    queryString = "("
    for cat in catList:
        queryString = queryString + "'" + cat + "', "
    queryString = queryString[:-2] + ")"
    return(queryString)
    

def sampleQAImageCat(userId,category):
    catQuery = getCatQuery(category)
    try:
        with conn.cursor() as cur:
            query = """SELECT img_id,text,img_http,qa_type from qa_sample where user_id = '""" + userId
            query2 = """' and sample_month = to_char(now(), 'mm') and time_submitted is NULL and qa_type in """
            cur.execute(query + query2 + catQuery)
            row = cur.fetchone()
            if(row==None):
                #print("no more qa questions for user %s" %(userId))
                return({})
            sampleDict = {
                'imgId':row[0],
                'text':row[1],
                'imgHttp':row[2],
                'qa':row[3]
            }
            #print("found qa question to sample for user %s" %(userId))
            return(sampleDict)
    except Exception as e:
        print(str(e))

def getCodeSample(userId):
    randPerc = random.randint(1,10)
    if(randPerc < 2):
        qaSample = sampleQAImage(userId)
        try:
            if(len(qaSample.keys())>0):
                return qaSample
        except Exception as e:
            print(str(e))
    #print("sample an image to code")
    sampleNum = getCurIndex()
    #print("sample from num: %i" %(sampleNum))
    try:
        needRecord = True
        while(needRecord):
            sampleNum+=1
            updateIndex(sampleNum)
            record = sampleRecordFromSQL(sampleNum-1)
            if(record['text'].find('19th child')==-1):
                if(record['text'].find('songs for babies')==-1):
                    needRecord = False
                    return record
    except Exception as e:
        print(str(e))



def getSample(userId,category):
    randPerc = random.randint(1,10)
    if(randPerc < 5):
        qaSample = sampleQAImageCat(userId,category)
        try:
            if(len(qaSample.keys())>0):
                return qaSample
        except Exception as e:
            print(str(e))
    sampleNum = getSampleIndex(category)
    try:
        needRecord = True
        while(needRecord):
            sampleNum+=1
            updateSampleIndex(sampleNum,category)
            if(category=='place'):
                record = samplePlaceRecordFromSQL(sampleNum-1)
            elif(category=='child'):
                 record = sampleChildRecordFromSQL(sampleNum-1)
            else:
                record = sampleHealthRecordFromSQL(sampleNum-1)
            if(record['text'].find('19th child')==-1):
                if(record['text'].find('songs for babies')==-1):
                    needRecord = False
                    return record
    except Exception as e:
        print(str(e))

def getSample2(userId,category):
    randPerc = random.randint(1,10)
    if(randPerc < 5):
        qaSample = sampleQAImageCat(userId,category)
        try:
            if(len(qaSample.keys())>0):
                return qaSample
        except Exception as e:
            print(str(e))
    sampleNum = getSampleIndex(category)
    try:
        needRecord = True
        while(needRecord):
            sampleNum+=1
            updateSampleIndex(sampleNum,category)
            if(category=='place'):
                record = samplePlaceRecordFromSQL2(sampleNum-1)
            elif(category=='child'):
                 record = sampleChildRecordFromSQL2(sampleNum-1)
            else:
                record = sampleHealthRecordFromSQL2(sampleNum-1)
            if(record['text'].find('19th child')==-1):
                if(record['text'].find('songs for babies')==-1):
                    needRecord = False
                    return record
    except Exception as e:
        print(str(e))


def processSubmission(inputDict):
    if(inputDict['qa']=='none'):
        #print("processing non qa record")
        addLabelRecord(inputDict)
    else:
        #print("processing qa record")
        processQARecord(inputDict)

def addLabelRecord(inputDict):
    try:
        with conn.cursor() as cur:
            query = """INSERT INTO twitter_labels (img_id,location,is_child,user_id,age,is_health,health_impact,health_type,location_cat,time_submitted)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
            ON CONFLICT (img_id,user_id) DO UPDATE SET location=EXCLUDED.location, is_child=EXCLUDED.is_child,age=EXCLUDED.age,
            is_health=EXCLUDED.is_health,health_impact=EXCLUDED.health_impact,health_type=EXCLUDED.health_type,location_cat = EXCLUDED.location_cat,
            time_submitted=EXCLUDED.time_submitted
            ;"""
            cur.execute(query,
                (
                    str(inputDict['imgId']),
                    inputDict['location'],
                    inputDict['isChild'],
                    inputDict['userId'],
                    inputDict['age'],
                    inputDict['isHealth'],
                    inputDict['healthImpact'],
                    inputDict['healthType'],
                    inputDict['locationCat'],
                )           
            )
            conn.commit()
            cur.close()
    except Exception as e:
        conn.rollback()
        print(str(e))

@app.route("/")
def hello():
    return "Hello World!"

@app.route("/sample", methods=['GET'])
def get_sample():
    sampleNum = request.args.get('id',default=0,type=int)
    try:
        return sampleRecordFromSQL(sampleNum)
    except Exception as e:
        print(str(e))

@app.route("/code", methods=['GET'])
def get_next():
    userId = request.args.get('user',default='guest',type=str)
    if(userId in validatedWokers):
        #print("%s is a valid worker" %(userId))
        sample = getCodeSample(userId)
        return(sample)
    else:
        #print("worker credentials could not be validated")
        return {
            'imgId':0,
            'text':'your user credentials could not be validated',
            'imgHttp':'SN.png',
            'qa':'none'
        }

@app.route("/place", methods=['GET'])
def get_place():
    userId = request.args.get('user',default='guest',type=str)
    if(userId in validatedWokers):
        print("%s is a valid worker" %(userId))
        sample = getSample2(userId,'place')
        return(sample)
    else:
        print("worker credentials could not be validated")
        return {
            'imgId':0,
            'text':'your user credentials could not be validated',
            'imgHttp':'SN.png',
            'qa':'none'
        }

@app.route("/child", methods=['GET'])
def get_child():
    userId = request.args.get('user',default='guest',type=str)
    if(userId in validatedWokers):
        sample = getSample2(userId,'child')
        return(sample)
    else:
        return {
            'imgId':0,
            'text':'your user credentials could not be validated',
            'imgHttp':'SN.png',
            'qa':'none'
        }

@app.route("/health", methods=['GET'])
def get_health():
    userId = request.args.get('user',default='guest',type=str)
    if(userId in validatedWokers):
        sample = getSample2(userId,'health')
        return(sample)
    else:
        return {
            'imgId':0,
            'text':'your user credentials could not be validated',
            'imgHttp':'SN.png',
            'qa':'none'
        }

@app.route('/submit', methods=['POST'])
def my_test_endpoint():
    inputDict = request.get_json(force=True) 
    processSubmission(inputDict)
    dictToReturn = {'answer':42}
    return jsonify(dictToReturn)


validatedWokers = getValidatedWorkers()


######### main function ############

if __name__ == '__main__':
    PORT = int(os.environ.get('PORT', 17995))
    print(PORT)
    app.run(host='0.0.0.0',port=PORT)

