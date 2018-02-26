import json
import json
import traceback
from http import HTTPStatus
from json import JSONDecodeError

from jassrealtime.search.document import DocumentSearch
from jassrealtime.webapi.handlers.base_handler import BaseHandler

from jassrealtime.core.master_factory_list import get_master_bucket_list
from jassrealtime.document.bucket import DocumentAlreadyExistsException, DocumentNotFoundException, \
    BucketNotFoundException
from jassrealtime.security.security_selector import get_autorisation
from jassrealtime.webapi.handlers.parameter_names import *
from jassrealtime.core.settings_utils import get_env_id, get_settings


class AnnotationFolderHandler(BaseHandler):
    def post(self, corpusId, bucketId):
        try:
            body = json.loads(self.request.body.decode("utf-8"))
            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)
            docType = None
            annotationId = None

            sett = get_settings()
            shouldValidate = sett['USE_ANNOTATION_AND_SCHEMA_VALIDATOR']

            if "annotationId" in body:
                annotationId = body["annotationId"]
                del body["annotationId"]

            if "schemaType" in body:
                docType = body["schemaType"]
            else:
                self.write_and_set_status(
                    {MESSAGE: "Missing schemaType field, which links the annotation to its schema."},
                    HTTPStatus.UNPROCESSABLE_ENTITY)
                return

            annotationId = get_master_bucket_list(envId, authorization) \
                .get_bucket(corpusId, bucketId) \
                .add_annotation(body, docType, annotationId, shouldValidate)

            self.write_and_set_status({"id": annotationId},
                                      HTTPStatus.OK)
        except BucketNotFoundException:
            self.write_and_set_status({MESSAGE: "Specified bucket not found"},
                                      HTTPStatus.NOT_FOUND)
        except DocumentAlreadyExistsException:
            self.write_and_set_status({MESSAGE: "Annotation with the same id already exist"},
                                      HTTPStatus.CONFLICT)
        except JSONDecodeError:
            self.write_and_set_status({MESSAGE: "Invalid JSON format for annotation"},
                                      HTTPStatus.BAD_REQUEST)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)

    def options(self, corpusId, bucketId):
        self.write_and_set_status(None, HTTPStatus.OK)

    def put(self, corpusId, bucketId):
        try:
            body = json.loads(self.request.body.decode("utf-8"))
            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)
            docType = None
            annotationId = None

            sett = get_settings()
            shouldValidate = sett['USE_ANNOTATION_AND_SCHEMA_VALIDATOR']

            if "annotationId" in body:
                annotationId = body["annotationId"]
                del body["annotationId"]
            else:
                self.write_and_set_status(
                    {MESSAGE: "Missing annotationId field required to find an annotation to update."},
                    HTTPStatus.UNPROCESSABLE_ENTITY)
                return

            if "schemaType" in body:
                docType = body["schemaType"]
            else:
                self.write_and_set_status(
                    {MESSAGE: "Missing schemaType field, which links the annotation to its schema."},
                    HTTPStatus.UNPROCESSABLE_ENTITY)
                return

            if "bucketId" in body:
                newBucketId = body["bucketId"]
                if newBucketId != bucketId:
                    self.write_and_set_status(
                        {MESSAGE: "bucketId from the path is different than bucketId in the body."},
                        HTTPStatus.UNPROCESSABLE_ENTITY)
                    return

            bucket = get_master_bucket_list(envId, authorization).get_bucket(corpusId, bucketId)
            storedAnnotation = bucket.get_annotation(id=annotationId, docType=docType)
            if storedAnnotation["schemaType"] != docType:
                self.write_and_set_status(
                    {MESSAGE: "You cannot change the schemaType of an annotation."},
                    HTTPStatus.UNPROCESSABLE_ENTITY)
                return

            bucket.update_annotation(body, docType, annotationId, shouldValidate)

            self.write_and_set_status(None,
                                      HTTPStatus.NO_CONTENT)
        except BucketNotFoundException:
            self.write_and_set_status({MESSAGE: "Specified bucket not found"},
                                      HTTPStatus.NOT_FOUND)
        except DocumentNotFoundException:
            self.write_and_set_status({MESSAGE: "Annotation with provided id does not exist"},
                                      HTTPStatus.NOT_FOUND)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)

    def delete(self, corpusId, bucketId):
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

            documentSearch.delete_annotations_for_types(bucketId, schemaTypes)

            self.write_and_set_status(None, HTTPStatus.NO_CONTENT)
        except BucketNotFoundException:
            self.write_and_set_status({MESSAGE: "Specified bucket not found"},
                                      HTTPStatus.NOT_FOUND)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)


class AnnotationHandler(BaseHandler):
    def get(self, corpusId, bucketId, annotationId):
        try:
            docType = self.get_argument("schemaType", None)
            if not docType:
                self.write_and_set_status({MESSAGE: "Missing schemaType."},
                                          HTTPStatus.UNPROCESSABLE_ENTITY)
                return

            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)
            anno = get_master_bucket_list(envId, authorization) \
                .get_bucket(corpusId, bucketId) \
                .get_annotation(annotationId, docType)

            annotationId = anno["id"]
            anno["annotationId"] = anno["id"]
            del anno["id"]
            self.write_and_set_status(anno,
                                      HTTPStatus.OK)
        except BucketNotFoundException:
            self.write_and_set_status({MESSAGE: "Specified bucket not found"},
                                      HTTPStatus.NOT_FOUND)
        except DocumentNotFoundException:
            self.write_and_set_status({MESSAGE: "Annotation with provided id and schemaType does not exist"},
                                      HTTPStatus.NOT_FOUND)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)

    def delete(self, corpusId, bucketId, annotationId):
        try:
            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)
            docType = self.get_argument("schemaType", None)
            if not docType:
                self.write_and_set_status(
                    {MESSAGE: "Missing schemaType field, which links the annotation to its schema."},
                    HTTPStatus.NOT_FOUND)
                return

            get_master_bucket_list(envId, authorization) \
                .get_bucket(corpusId, bucketId) \
                .delete_annotation(annotationId, docType)
            self.write_and_set_status(None,
                                      HTTPStatus.NO_CONTENT)
        except BucketNotFoundException:
            self.write_and_set_status({MESSAGE: "Specified bucket not found"},
                                      HTTPStatus.NOT_FOUND)
        except DocumentNotFoundException:
            self.write_and_set_status({MESSAGE: "Annotation with provided id does not exist"},
                                      HTTPStatus.NOT_FOUND)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)

    def options(self, corpusId, bucketId, annotationId):
        self.write_and_set_status(None, HTTPStatus.OK)
