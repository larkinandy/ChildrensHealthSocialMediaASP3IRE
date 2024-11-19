![GitHub Logo](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/blob/main/images/1x/TrainingDataExample.png)

# Create Training Dataset
Strategically sample database for tweets to label and create a website and backend SQL database for labeling tweets.  Finally, perform QA analysis and screen records with low confidence of quality from training dataset.


**Repository Structure** <br>
This folder contains three subfolders and one word document. One folder contains scripts and datasets for strategically sampling tweets and analyzing labeled records.  The second folder contains code for the frontend react JS website used to label tweets.  The third folder contains the backend server and SQL database for loading and storing training dataset records.

- **[analysis](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/deep_learning/create_training_dataset/analysis)** - strategically sample tweets from Neo4j database related to children, safe places, and health.  Analyze labeled records and peform QA analyses.  <br>
- **[website backend](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/deep_learning/create_training_dataset/website_backend)** - server for retreiving and storing training records from SQL database  <br>
- **[website_frontend](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/deep_learning/create_training_dataset/website_frontend)** - react JS website for labeling social media records. <br>
- **[Twitter Coding Guide](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/blob/main/deep_learning/create_training_dataset/Twitter%20Coding%20Guide_Redacted_20221026.docx)** - Instructions given to workers for labeling tweets. Tweets in the guide have been redacted due to privacy concerns and to comply with the Twitter Academic Research user agreement.

**External Links**
- **ReactJS** - https://react.dev/
- **PostgreSQL** - https://www.postgresql.org/
- **Heroku** - https://www.heroku.com/
- **AmazonS3 bucket stroage** - https://aws.amazon.com/s3/
