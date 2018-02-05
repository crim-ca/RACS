# coding: utf-8

# Why to use ini when you can use python directly, especially since it is READ ONLY
# http://stackoverflow.com/questions/8225954/python-configuration-file-any-file-format-recommendation-ini-format-still-appr

import os

JASS_LOG_LEVEL = os.environ.get("JASS_LOG_LEVEL", "info")  # possible log levels: debug,info,warning,error
_ES_HOST = os.environ.get("ES_HOST", "localhost")
JASS_ENV = os.environ.get("JASS_ENV", "jassdev")
FILE_STORAGE_DATA_URL = os.environ.get("FILE_STORAGE_DATA_URL", "http://localhost:6999")
JASS_NB_CORES = os.environ.get("JASS_NB_CORES", "1");
JASS_TMP_DIR = os.environ.get("JASS_TMP_DIR", "/tmp")  # Folder used by jass to temporarely store uploaded files
SCAN_SCROLL_DURATION = os.environ.get("SCAN_SCROLL_DURATION", "15m")
NB_DOCUMENTS_PER_SCAN_SCROLL = int(os.environ.get("SCAN_SCROLL_DURATION", "1000"))
NUMBER_OF_SHARDS = int(os.environ.get("NUMBER_OF_SHARDS", "1"))
NUMBER_OF_REPLICAS = int(os.environ.get("NUMBER_OF_REPLICAS", "0"))
JASS_EXPOSE_SWAGGER = os.environ.get("JASS_EXPOSE_SWAGGER", "True")

JASS_ALLOW_CORS = True
if os.environ.get("JASS_ALLOW_CORS", "True") == "True":
    JASS_ALLOW_CORS = True
else:
    JASS_ALLOW_CORS = False

JASS_REBUILD_ENV = True
if os.environ.get("JASS_REBUILD_ENV", "True") == "True":
    JASS_REBUILD_ENV = True
else:
    JASS_REBUILD_ENV = False

JASS_MANAGE_ENV = True
if os.environ.get("JASS_MANAGE_ENV", "True") == "True":
    JASS_MANAGE_ENV = True
else:
    JASS_MANAGE_ENV = False

_SETTINGS = {
    'SERVER_NAME': "localhost",
    'USE_ANNOTATION_AND_SCHEMA_VALIDATOR': False,
    # If true, use an external validator to validate annotations and schemas.
    'ELASTIC_SEARCH': {
        'hosts': [_ES_HOST],
        "sniff_on_start": True,
        "sniff_on_connection_fail": True,
        "sniffer_timeout": 60,
        "sniff_timeout": 30,
        "cluster_health_timeout": 60,
        # Maximal number of connections to each elastic search node
        "maxsize": 25,
        "scan_scroll_duration": SCAN_SCROLL_DURATION
    },
    'CLASSES': {
        'ENV': {
            'MASTER_ENV_ID': 'envs_list'  # contains all envs. should not be modified
        },
        'DOCUMENT_DIRECTORY': {
            'CLASS_PREFIX': "_dd_",  # used to distinguish indexes created by this class
            'INDEX_DATA_SUFFIX': "_data",  # Suffix to indicated index containing data
            'INDEX_TYPE_SUFFIX': "_type",  # Suffix to indicated index containing type mappings
            'INDEX_DEFAULT_TYPE_SUFFIX': '_0',  # Document without type will be put to this index.
            'DEFAULT_TYPE': "default",  # Type for documents without type. DO NOT CHANGE THIS VALUE
            'DIRECTORY_DOC_TYPE': "directory",  # Type of DocumentDirectory objects inside master index
            'MASTER_DIRECTORY_INDEX_SUFFIX': "document_directory_listing"
            # Index to list all directories. Combined with envId and class prefix to create a master index.
        },
        'SCHEMA_LIST': {
            'CLASS_PREFIX': "_schema_list_",  # used to distinguish indexes created by this class
            'MASTER_JSON_SCHEMA_SUFFIX': "json_schemas",  # Index of all user defined json schemas.
            'MASTER_ES_SCHEMA_SUFFIX': "es_schemas",  # Index to list all schemas
            'DEFAULT_LANGUAGE_ANALYSERS': ["arabic", "armenian", "basque", "brazilian", "bulgarian", "catalan", \
                                           "cjk", "czech", "danish", "dutch", "english", "finnish", "french", \
                                           "galician", "german", "greek", "hindi", "hungarian", "indonesian", \
                                           "irish", "italian", "latvian", "lithuanian", "norwegian", "persian", \
                                           "portuguese", "romanian", "russian", "sorani", "spanish", "swedish", \
                                           "turkish", "thai"]
        },
        'DOCUMENT_CORPUS': {
            'CLASS_PREFIX': "_dc_",
            'MASTER_DOCUMENT_CORPUS_ID': 'document_corpus_listing',  # Lists all corpuses
            'DOCUMENT_SUB_CORPUS': {
                'CLASS_PREFIX': "_dsc_",
                'MASTER_DOCUMENT_SUB_CORPUS_ID': 'document_sub_corpus_listing'  # Lists all sub corpuses
            }
        },
        'BUCKET': {
            'CLASS_PREFIX': "_b_",
            'MASTER_BUCKET_ID': 'bucket_directory_list',
            'BUCKET_BINDING_INDEX_SUFFIX': 'bucket_bindings'
        },
        'BATCH': {
            "MAX_ANNOTAION_BULK_SIZE": 10000000
            # maximum size of annotations sent to elastic search bulk method. Default 10MB
        },

    },
    'STORAGE_TYPE': {
        # Simple file server which stores data in a shared directory with FileStorage
        # (Or sharing the same volume in docker)
        'SIMPLE_FILE_SERVER': {
            "SHARED_DIR_PATH": "/data"  # Path for shared directory
        }
    },
    'LANGUAGE': {
        # This dictionary maps language used by user to a correct elastic search analyser.
        'LANGUAGE_TO_ES_CONVERSION': {
            'en': "english",
            'fr': "french",
            'en-*': "english",
            'fr-*': "french",
            "english": "english",
            "french": "french",
        }
    }
}
