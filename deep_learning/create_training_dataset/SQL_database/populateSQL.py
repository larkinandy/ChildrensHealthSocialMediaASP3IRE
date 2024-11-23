### populateSQL.py
### Author: Andrew Larkin
### Date Created: November 22nd, 2024
### Summary: Add Twitter records to be labeled to SQL database

# import libraries
import pandas as ps
import psycopg2
import pandas.io.sql as psql

# custom driver for querying SQL database
from SQL_Class import SQL_DB

# load runtime arguments
debug = sys.argv[1] # true or false, whether to run in debug or deployment mode
db = sys.argv[2] # SQL param - database
user = sys.argv[3] # SQL param - username
pw = sys.argv[4] # SQL param - password
host = sys.argv[5] # SQL param - host
csvFilepath = sys.argv[6] # where twitter records are stored

dbDict = {
    'host':host,
    'db':db,
    'user':user,
    'pw':pw
}

# load new tweets into SQL database. Each category of tweet (place, child, health) has 
# a unique table
# INPUTS:
#    driver (SQ_DB) - custom class for querying SQL database
#    tweetData (pandas dataframe) - contains tweets to insert into db
#    category (str) - value for the column 'category', which category of tweets to insert
#    tableName (str) - name of the table to insert records into
def loadNewTweets(driver,tweetData,category,tableName):
    categoryTweets = tweetData[tweetData['category']==category]
    print("number of tweets to relabel: %i" %(categoryTweets.count()[0]))
    for index in range(categoryTweets.count()[0]):
        curRecord = categoryTweets.iloc[index]
        driver.insertSingleTweet(curRecord,tableName)
        if(index%1000==0):
            print(index)

# main function
if __name__ == "__main__":

    # debug on local SQL database
    if(debug=='True'):
        print("enterting debug mode")
        driver = SQL_DB(dbDict,True)

    # query deployment database in the cloud
    else:
        print("entering deployment mode")0
        driver = SQL_DB(dbDict,False)

    tweetData = ps.read_csv(csvFilepath)
    loadNewTweets(driver,tweetData,'place',"place_tweets3")
    loadNewTweets(driver,tweetData,'age',"age_tweets3")
    loadNewTweets(driver,tweetData,'health',"health_tweets3")

# end of populateSQL.py