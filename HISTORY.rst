.. :changelog:

History
=======
2.6.7.4
---------------------
* Added JASS_ES_CONNECTION_TIMEOUT environement variable to control jass connection and request timeout to elastic search.

2.6.7
---------------------
* Document search by annotations

2.6.6
---------------------
* Add flag to delete document's annotation when deleting the document

2.6.5
---------------------
* Document search by text

2.6.4
---------------------
* Expose query structure for document search by text and annotations

2.6.3
---------------------
* Optimization annotation count for types

2.6.2
---------------------
* Bugfix string array properties

2.6.1
---------------------
* Bugfix get documents zip

2.6.0
---------------------
* Elasticsearch 2.4.1 -> 6.1.2
* Alpine image 3.4 -> 3.7 to use Python 3.6

2.5.9
---------------------
* Change historic JASS name to RACS in Swagger API

2.5.8
---------------------
* Add possibility to expose or not the Swagger endpoint via an environment variable.

2.5.7
---------------------
* Bugfix: zip exportation of a lot of documents or annotations.

2.5.5
---------------------
* When deleting a schema from a bucket, delete associated annotations.
* Number of shard and replicas can be specified via environment variables.

2.5.0
---------------------
* API change for zip transit of documents and annotations.

2.3.4
---------------------
* Bug fix: get corpus document ids returns only 10 ids.

2.3.3
---------------------
* First structured release.
