import traceback
from http import HTTPStatus

from jassrealtime.core.settings_utils import get_env_id
from jassrealtime.document.bucket import BucketNotFoundException
from jassrealtime.search.document import DocumentSearch
from jassrealtime.security.security_selector import get_autorisation
from jassrealtime.webapi.handlers.base_handler import BaseHandler
from jassrealtime.webapi.handlers.parameter_names import MESSAGE, TRACE


class AnnotationCountHandler(BaseHandler):
    def options(self, corpusId, bucketId):
        self.write_and_set_status(None, HTTPStatus.OK)

    def get(self, corpusId, bucketId):
        try:
            schemaTypesArgument = self.get_query_argument("schemaTypes", default=None)
            if not schemaTypesArgument:
                self.write_and_set_status({MESSAGE: "Missing schemaTypes parameter"},
                                          HTTPStatus.UNPROCESSABLE_ENTITY)
                return
            else:
                schemaTypes = schemaTypesArgument.split(",")

            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)
            documentSearch = DocumentSearch(envId, authorization, None, corpusId)

            counts = documentSearch.count_annotations_for_types(bucketId, schemaTypes)

            self.write_and_set_status(counts, HTTPStatus.OK)
        except BucketNotFoundException:
            self.write_and_set_status({MESSAGE: "Specified bucket not found"},
                                      HTTPStatus.NOT_FOUND)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)
