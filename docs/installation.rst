============
Installation
============

Running RACS stand alone with compose
=====================================

Prerequisites
-------------

-  Docker 1.11+ (Untested for earlier versions). See
   https://docs.docker.com/engine/installation/

To use RACS clone the repo, then get inside and run

.. code:: bash

    docker-compose up -d

This will start RACS on port 8888, file storage services on port 6999, and elasticsearch on port 9200,9300.

To stop all services

.. code:: bash

    # go to repos root
    cd jass
    docker-compose stop

Testing api with swagger
------------------------

Swagger is used to describe RACS rest API. Once RACS is running go to:
http://localhost:8888/static/web/swagger/index.html in order to test it.

Updating RACS
=============

Update the repo, then:

.. code:: bash

    # go to repos root
    cd jass
    # this will stop, remove and rebuild RACS image
    docker-compose stop jass_real_time && docker-compose up -d --no-deps --build jass_real_time

Dev Installation Instructions
=============================

This section is to help developers set up their project.

Prerequisites
-------------

-  pip (latest)

-  Python 3.5.x

-  Docker 1.11+ (Untested for earlier versions)

Installation
~~~~~~~~~~~~

To use RACS clone the repo, then get inside and run Install requirements

.. code:: bash

    pip install -r requirements.txt

Start elastic search and file storage services
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    cd dev
    docker-compose up -d

Start RACS
~~~~~~~~~~

Get to repo root and run

.. code:: bash

    python -m jassrealtime.webapi.app

Packages Description
--------------------

-  jass\_real\_time: Main packger used to store a limited amount of
   annotations in real time.

   -  core: Core jias functionnality. Contains notions of Schema and
      Generic Elastic seach document.

      .. code::

          Also contains settings.py which has the default settings for the project. TODO: move it in a specific dir.

   -  document: Corpus,Bucket,Annotations

   -  security: Security related to data access. TODO: rename the class
      to pass through security.

   -  search: Search API to get a subset of annotations based on search

   -  batch: Access of annotations / documents in batch.

   -  webapi: Web service.

-  jasstests: Unitests are here. Mimics the JASSRealTime, to create
   tests for specific classes.

Unit Tests
----------

Unit tests are located in *jasstests* directory.

Updating swagger
----------------

Swagger is used to document RACS.

In order to update swagger documentation change the file
jassrealtime/webapi/static/web/json/jass\_swagger.json

For developement purposes it is easier to work with yaml files. We
advise to use jassrealtime/webapi/swagger\_editor\_format.yaml inside a
yaml editor like: http://editor.swagger.io/#/, then export the json
results.
