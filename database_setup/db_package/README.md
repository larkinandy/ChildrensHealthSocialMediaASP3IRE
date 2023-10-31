<img src="https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/blob/main/images/db_setup/db_structure.png" width="500">

### database_setup
This github repository contains python scripts and custom classes for ingesting social media records into a Neo4j database and researching attitudes, perceptions, behaviors, and children's environmental health social media misinformation.

**Repository Structure** <br>
Files are divided into two folders, with each folder corresponding to a unique stage of database development

- **[initialization](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/database_setup/initialization)** - create a Neo4j database and define node and relationship types.  Also includes documents pertinent to database architecture <br>
- **[db_package](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/database_setup/db_package)** - a suite of classes for ingesting data from social media into the Neo4j databse and updating node properties and relationships as new information becomes available (e.g. new labels from deep learning models). <br>

**External Links**
- **Neo4j** - [https://neo4j.com/)
- **Graph Database** - https://neo4j.com/developer/graph-database/
