import traceback
from http import HTTPStatus

from jassrealtime.core.settings_utils import get_env_id
from jassrealtime.search.document import *
from jassrealtime.search.multicorpus.multi_corpus import get_annotations_of_type
from jassrealtime.security.security_selector import get_autorisation
from jassrealtime.webapi.handlers.base_handler import BaseHandler
from jassrealtime.webapi.handlers.parameter_names import *

# Elastic search limit values for long
SCHEMA_TYPE_DOCUMENT_METADATA = "DOCUMENT_META"
MIN_OFFSET_BEGIN = -9223372036854775808
MAX_OFFSET_END = 9223372036854775807


class DocumentSearchHandlerBase(BaseHandler):
    def getAnnotations(self, corpusId, documentIds: List[str]):
        schemaTypesByBucketId = {}

        try:
            schemaTypes = self.get_arguments("schemaTypes")
            if not schemaTypes:
                self.write_and_set_status({MESSAGE: "Missing schemaTypes parameter"},
                                          HTTPStatus.UNPROCESSABLE_ENTITY)
                return
            schemaTypes = schemaTypes[0].split(",")  # tornado crap syntax
            for bucketIdWithSchemaType in schemaTypes:
                bucketId = bucketIdWithSchemaType.split(":")[0]
                schemaType = bucketIdWithSchemaType.split(":")[1]
                if not bucketId in schemaTypesByBucketId:
                    schemaTypesByBucketId[bucketId] = []
                schemaTypesByBucketId[bucketId].append(schemaType)
        except Exception as e:
            self.write_and_set_status({MESSAGE: "Invalid data passed in schemaTypes parameter"},
                                      HTTPStatus.UNPROCESSABLE_ENTITY)
            return

        # TODO i put some arbitrary large number for offset if not present
        offsetBegin = self.get_argument("offsetBegin", MIN_OFFSET_BEGIN)
        offsetEnd = self.get_argument("offsetEnd", MAX_OFFSET_END)

        envId = get_env_id()
        authorization = get_autorisation(envId, None, None)
        documentSearch = DocumentSearch(envId, authorization, documentIds, corpusId)
        offsets = None
        if not (offsetBegin == MIN_OFFSET_BEGIN and offsetEnd == MAX_OFFSET_END):
            offsets = [Interval(offsetBegin, offsetEnd, False, False, False)]
        res = documentSearch.get_annotations(schemaTypesByBucketId, offsets)
        if not res[corpusId]:
            self.write({})
        else:
            self.write(res)
        self.write_and_set_status(None,
                                  HTTPStatus.OK)


class DocumentSearchHandler(DocumentSearchHandlerBase):
    def options(self, corpusId, documentId):
        self.write_and_set_status(None, HTTPStatus.OK)

    def get(self, corpusId, documentId):
        try:
            documentIds = [documentId]
            self.getAnnotations(corpusId, documentIds)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)


class DocumentFolderSearchHandler(DocumentSearchHandlerBase):
    def options(self, corpusId):
        self.write_and_set_status(None, HTTPStatus.OK)

    def get(self, corpusId):
        try:
            documentIds = self.get_arguments("documentIds")
            if not documentIds or not documentIds[0]:
                documentIds = None
            else:
                documentIds = documentIds[0].split(",")
        except Exception as e:
            self.write_and_set_status({MESSAGE: "Invalid data passed in documentIds parameter"},
                                      HTTPStatus.UNPROCESSABLE_ENTITY)
            return

        try:
            self.getAnnotations(corpusId, documentIds)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)


def parse_filters_argument(filters_argument):
    if not filters_argument:
        return None

    filters = []
    filter_expressions = filters_argument.split(",")
    for expression in filter_expressions:
        name_value = expression.split(":")
        filters.append((name_value[0], name_value[1]))

    return filters


class SingleTypeDocumentSearchHandler(BaseHandler):
    def options(self, corpusId, bucketId, schemaType):
        self.write_and_set_status(None, HTTPStatus.OK)

    def get(self, corpusId, bucketId, schemaType):
        try:
            fromIndexArgument = self.get_query_argument("from")
            fromIndex = int(fromIndexArgument)
            if fromIndex < 0:
                self.write_and_set_status({MESSAGE: "'from' must cannot be less than zero"},
                                          HTTPStatus.UNPROCESSABLE_ENTITY)
                return

            sizeArgument = self.get_query_argument("size")
            size = int(sizeArgument)

            if size < 1:
                self.write_and_set_status({MESSAGE: "'size' cannot be less than 1"},
                                          HTTPStatus.UNPROCESSABLE_ENTITY)
                return

            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)
            documentSearch = DocumentSearch(envId, authorization, None, corpusId)

            filters = parse_filters_argument(self.get_query_argument("filters", default=None))
            filterJoin = self.get_query_argument("filterJoin", default=None)
            sortBy = self.get_query_argument("sortBy", default=None)
            sortOrder = self.get_query_argument("sortOrder", default=None)

            count, annotations = documentSearch.search_annotations_for_one_type(
                bucketId, schemaType,
                fromIndex, size, sortBy, sortOrder, filters, filterJoin)

            self.write_and_set_status({
                "count": count,
                "annotations": annotations},
                HTTPStatus.OK)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)


class DocumentMetadataSearchHandler(BaseHandler):
    def options(self):
        self.write_and_set_status(None, HTTPStatus.OK)

    def get(self):
        try:
            fromIndexArgument = self.get_query_argument("from")
            fromIndex = int(fromIndexArgument)
            if fromIndex < 0:
                self.write_and_set_status({MESSAGE: "'from' must cannot be less than zero"},
                                          HTTPStatus.UNPROCESSABLE_ENTITY)
                return

            sizeArgument = self.get_query_argument("size")
            size = int(sizeArgument)

            if size < 1:
                self.write_and_set_status({MESSAGE: "'size' cannot be less than 1"},
                                          HTTPStatus.UNPROCESSABLE_ENTITY)
                return

            try:
                corpusIdsArgument = self.get_query_argument("corpusIds", default=None)
                if not corpusIdsArgument:
                    self.write_and_set_status({MESSAGE: "Missing corpusIds parameter"},
                                              HTTPStatus.UNPROCESSABLE_ENTITY)
                    return
                else:
                    corpusIds = corpusIdsArgument.split(",")
            except Exception as e:
                self.write_and_set_status({MESSAGE: "Invalid data passed in corpusIds parameter"},
                                          HTTPStatus.UNPROCESSABLE_ENTITY)
                return

            filters = parse_filters_argument(self.get_query_argument("filters", default=None))
            filterJoin = self.get_query_argument("filterJoin", default=None)
            sortBy = self.get_query_argument("sortBy", default=None)
            sortOrder = self.get_query_argument("sortOrder", default=None)

            count, annotations = get_annotations_of_type(
                corpusIds, SCHEMA_TYPE_DOCUMENT_METADATA,
                fromIndex, size, sortBy, sortOrder, filters, filterJoin)

            self.write_and_set_status({
                "count": count,
                "annotations": annotations},
                HTTPStatus.OK)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)
