<img src="https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/blob/main/images/analysis/Network.png">

### analysis
python scripts and custom classes for analyzing social media records.  Analyses include identifying communities within networks, topic modeling, and (mis)information propagation

**Repository Structure** <br>
Files are contained within a single folder.  Functions are partitioned into classes based on focus area

- **[AnalysisClass](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/analysis/AnalysisClass.py)** - perform common database queries, such as calculating the number of tweets that contain a keyword <br>
- **[NetworkClass](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/analysis/NetworkClass.py)** - identify communities and calculate social network metrics <br>
- **[TopicClass](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/analysis/TopicClass.py)** - using Top2Vec, identify topics discussed in social media posts <br>
- **[topics_words.npy](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/analysis/topics_words.npy)** - set of 50 words that collectively uniquely identify each topic vector <br>

**External Links**
- **Topic model reduced to 5000 topics** - https://drive.google.com/file/d/1vGHkTLB5RAy9FOqDy2qogbnO88_KcdZG/view?usp=drive_link
- **Neo4j** - https://neo4j.com/
- **NetworkX** - https://networkx.org/
- **cuGraph** - https://github.com/rapidsai/cugraph
- **Top2Vec** - https://github.com/ddangelov/Top2Vec
