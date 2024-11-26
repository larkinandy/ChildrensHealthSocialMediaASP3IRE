<img src="https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/blob/main/images/1x/TrainingDataExample.png" width=1000>

# Create Training Dataset
Strategically sample database for tweets to label and create a website and backend SQL database for labeling tweets.  Finally, perform QA analysis and screen records with low confidence of quality from training dataset.


**Repository Structure** <br>

- **[analysis](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/deep_learning/create_training_dataset/analysis)** - Analyze labeled records and peform QA analyses.  <br>
- **[imageAugmentation](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/main/deep_learning/create_training_dataset/imageAugmentation)** - rotate and change color of training images. Clip faces out of images for age model 
- **[labeled_datasets](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/main/deep_learning/create_training_dataset/labeled_datasets)** - coded tweets after QA filtering
- **[tf_records](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/main/deep_learning/create_training_dataset/tf_records)** - create tf records for model training
- **[website backend](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/deep_learning/create_training_dataset/website_backend)** - server for retreiving and storing training records from SQL database  <br>
- **[website_frontend](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/deep_learning/create_training_dataset/website_frontend)** - react JS website for labeling social media records. <br>
- **[SQL_database](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/main/deep_learning/create_training_dataset/SQL_database)** - scripts for creating, querying, and populating SQL datbabase
- **[Twitter Coding Guide](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/blob/main/deep_learning/create_training_dataset/Twitter%20Coding%20Guide_Redacted_20221026.docx)** - Instructions given to workers for labeling tweets. Tweets in the guide have been redacted due to privacy concerns and to comply with the Twitter Academic Research user agreement.

**External Links**
- **ReactJS** - https://react.dev/
- **PostgreSQL** - https://www.postgresql.org/
- **Heroku** - https://www.heroku.com/
- **AmazonS3 bucket storage** - https://aws.amazon.com/s3/
- **TFRecords** - https://www.kaggle.com/code/ryanholbrook/tfrecords-basics
- **Image Augmentation** - https://www.tensorflow.org/tutorials/images/data_augmentation
