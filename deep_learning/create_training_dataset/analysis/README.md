![GitHub Logo](/Images/Matching.jpg )

# Analysis
QA results of workers who labeled the training dataset

**Summary** <br>
Analyses are stored in Excel files. Scripts used to perform statistical analyses are not included, as all derived metrics (e.g. accuracy, precision) are widely known.

**Repository Structure** <br>
Repository consists of python files with code for performing analyses, and anlyses results in Excel files.

- **[Twitter_Labeling_QA_Analysis](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/blob/main/deep_learning/create_training_dataset/analysis/Twitter_Labeling_QA_Analysis.xlsx)** - QA performance for each worker that labeled tweets, stratified by deep learning model label.<br>
- **[filterTrainingRecords.py](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/blob/main/deep_learning/create_training_dataset/analysis/filterTrainingRecords.py)** - Query records from SQL database, clean records, and store in csv <br>
- **[calcWeeklyPerformance.py](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/blob/main/deep_learning/create_training_dataset/analysis/calcWeeklyPerformance.py)** - Calculate performance of workers for a given time period. Performance metrics include productivity and accuracy.  <br>
- **[QA_Analysis.py](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/blob/main/deep_learning/create_training_dataset/analysis/QA_Analysis.py)** - Calculate QA question responses for the entire worker cohort. <br>

TODO:
- add summary figure/graph/table
