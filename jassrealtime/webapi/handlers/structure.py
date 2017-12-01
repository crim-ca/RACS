import json
import traceback
from http import HTTPStatus

from jassrealtime.webapi.handlers.base_handler import BaseHandler

from jassrealtime.core.master_factory_list import get_master_document_corpus_list
from jassrealtime.core.settings_utils import get_env_id
from jassrealtime.document.document_corpus import CorpusNotFoundException
from jassrealtime.security.security_selector import get_autorisation
from jassrealtime.webapi.handlers.parameter_names import MESSAGE, TRACE

# Parameter names
INCLUDE_SCHEMA_JSON = "includeSchemaJson"


def getBucketWithSchema(bucket, includeSchemaJson: bool):
    augmentedBucket = {"name": bucket.name, "id": bucket.id}
    schemas = bucket.get_schemas_info(includeSchemaJson)
    augmentedBucket["schemas"] = schemas["data"]
    return augmentedBucket


class StructureHandler(BaseHandler):
    def options(self, corpusId):
        self.write_and_set_status(None, HTTPStatus.OK)

    def get(self, corpusId):
        try:
            includeSchemaJson = 'true' == self.get_query_argument(INCLUDE_SCHEMA_JSON, default=False)

            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)
            buckets = get_master_document_corpus_list(envId, authorization).get_corpus(corpusId).get_buckets()
            augmentedBuckets = [getBucketWithSchema(bucket, includeSchemaJson) for bucket in buckets]

            self.write_and_set_status({"buckets": augmentedBuckets},
                                      HTTPStatus.OK)
        except CorpusNotFoundException:
            self.write_and_set_status({MESSAGE: "Specified corpus not found"},
                                      HTTPStatus.NOT_FOUND)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)
