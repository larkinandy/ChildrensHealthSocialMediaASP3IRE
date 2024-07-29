### db_package
Contains custom classes for interacting with the Neo4j social media database. Example interactions include ingesting records from Twitter, creating nodes and relationships, updating node properties, and performing common queries for database analysis and maintanance.  

**Repository Structure** <br>
Each python script contains a single class responsible for a specific subset of database tasks or interacting with a specific category of node.

- **[ConversationNodeClass](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/database_setup/db_package/ConversationNodeClass.py)** Perform operations on conversation nodes . <br>
- **[GeoNodeClass](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/database_setup/db_package/GeoNodeClass.py)** - Perform operations on nodes used to store OpenStreetMap, US Census, and American Community Survey information  <br>
- **[GraphDBClass](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/database_setup/db_package/GraphDBClass.py)** A parent class with high level functions used to interact with lower level functions in other classes. <br>
- **[LabelNodeClass](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/database_setup/db_package/LabelNodeClass.py)** - perform operations on Label Nodes (e.g. nodes that represent keywords, safe places, child age groups, etc). <br>
- **[TimeNodeClass](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/database_setup/db_package/TimeNodeClass.py)** Perform operations on nodes used to store time information . <br>
- **[TweetImageClass](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/database_setup/db_package/TweetImageClass.py)** - Perform operations on nodes used to store social media image information  <br>
- **[TweetIngestClass](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/database_setup/db_package/TweetIngestClass.py)** Perform operations to ingest Twitter records into the database. <br>
- **[TweetNodeClass](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/database_setup/db_package/TweetNodeClass.py)** - Perform operations on nodes used to store tweet information (text, metadata, and image id) <br>
- **[TweetPlaceClass](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/database_setup/db_package/TweetPlaceClass.py)** Perform operations on nodes used to store Twitter places. <br>
- **[TwitterAPI](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/database_setup/db_package/TwitterAPI.py)** - Operations for querying the Twitter API <br>
- **[UserNodeClass](https://github.com/larkinandy/ChildrensHealthSocialMediaASP3IRE/tree/master/database_setup/db_package/UserNodeClass.py)** Perform operations on nodes used to store Twitter user information (e.g. username, self-reported location). <br>
