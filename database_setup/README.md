<img src="https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/blob/main/images/db_setup/db_structure.png" width="500">

### database_setup
Analyze social media records to gain insights into children's environmental health

**Summary** <br>
This github repository contains python scripts and custom classes for ingesting social media records into a Neo4j database and researching attitudes, perceptions, behaviors, and children's environmental health social media misinformation.

**Repository Structure** <br>
Files are divided into five folders, with each folder corresponding to a unique stage of the research pipeline

- **[database setup](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/database_setup)** - ingest records from X (formerly Twitter) and GIS datasets into a Neo4j database. Additional operations include processing social media records (e.g. georeferencing records) <br>
- **[deep learning](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/deep_learning)** - multimodal models for extracting information from X text and imagery <br>
- **[analysis](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/analysis)** - analyzing records for trends related to children's environmental health. <br>
- **[website](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/website)** - interactive website for viewing analysis results. <br>

**External Links**
- **Publications** - TODO: insert link once published
- **Funding** - [NIH/NIEHS](https://www.niehs.nih.gov/), GRANT13248774
- **OpenStreetMap** - https://www.openstreetmap.org/
- **X (formerly Twitter)** - https://twitter.com/home
- **Advancing Science, Practice, Programming and Policy in Research Translation for Children’s Environmental Health (ASP<sup>3</sup>IRE) Center** - https://health.oregonstate.edu/asp3ire
- **NVIDIA Accelerator Research Program** - https://www.nvidia.com/en-us/industries/higher-education-research/applied-research-program/
  
**Related Publications**
- [Integrating Geospatial Data and Social Media in Bidirectional Long-Short Term Memory Models to Capture Human Nature Interactions](https://academic.oup.com/comjnl/article/65/3/667/5893915)
- [Measuring and modelling perceptions of the built environment for epidemiological research using crowd-sourcing and image-based deep learning models](https://www.nature.com/articles/s41370-022-00489-8)

**Related Repositories**
- https://github.com/larkinandy/GreenTweet_MultivariateBiLSTM - predict nature perceptions and use from Twitter records, and link to OpenStreetMap
- https://github.com/larkinandy/Portland_UrbanNature_Twitter - pilot study analyzing self-reported urban nature trends in Portland, OR from Twitter posts.
