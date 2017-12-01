import json
import traceback
from http import HTTPStatus

from jassrealtime.webapi.handlers.base_handler import BaseHandler

from jassrealtime.core.master_factory_list import get_master_document_corpus_list, get_schema_list
from jassrealtime.security.security_selector import get_autorisation
from jassrealtime.webapi.handlers.parameter_names import *
from jassrealtime.core.settings_utils import get_env_id
from .utils import is_missing_required_fields, missing_fields_message
from jassrealtime.document.bucket import TargetType
from jassrealtime.core.schema_list import EsSchemaMigrationInvalidException, \
    EsSchemaMigrationDeleteFieldsNotSupportedException, SchemaBindingInvalid
from jassrealtime.document.document_corpus import CorpusNotFoundException
from jassrealtime.document.bucket import BucketNotFoundException


class BucketSchemaDeleteHandler(BaseHandler):
    def delete(self, corpusId, bucketId,schemaType):
        try:
            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)

            bucket = get_master_document_corpus_list(envId, authorization).get_corpus(corpusId).get_bucket(bucketId)
            schemas = bucket.get_schemas_info(False)
            schemaTypes = [schema['schemaType'] for schema in schemas['data']]
            if not schemaType in schemaTypes:
                self.write_and_set_status({MESSAGE: "Schema Type: {0} does not exist".format(schemaType)},
                                          HTTPStatus.NOT_FOUND)
                return

            bucket.delete_schema_type(schemaType)
            self.write_and_set_status(None, HTTPStatus.NO_CONTENT)
        except CorpusNotFoundException as err:
            self.write_and_set_status({MESSAGE: "Corpus does not exist.Extra info: '{0}'".format(err)},
                                      HTTPStatus.NOT_FOUND)
        except BucketNotFoundException as err:
            self.write_and_set_status({MESSAGE: "Bucket does not exist.Extra info: '{0}'".format(err)},
                                      HTTPStatus.NOT_FOUND)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)

    def options(self, corpusId, bucketId,schemaType):
        self.write_and_set_status(None, HTTPStatus.OK)

class BucketSchemaHandler(BaseHandler):
    def post(self, corpusId, bucketId):
        try:
            body = self.strip_body_bom()
            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)

            if is_missing_required_fields(body, ["targetType", "schemaType", "properties"]):
                self.write_and_set_status(
                    {MESSAGE: missing_fields_message(body, ["targetType", "schemaType", "properties"])},
                    HTTPStatus.UNPROCESSABLE_ENTITY)
                return

            schemaType = body["schemaType"]
            targetTypeName = body["targetType"]
            if not TargetType.has(targetTypeName):
                self.write_and_set_status({MESSAGE: "Target type {0} not supported".format(targetTypeName)},
                                          HTTPStatus.UNPROCESSABLE_ENTITY)
                return

            bucket = get_master_document_corpus_list(envId, authorization).get_corpus(corpusId).get_bucket(bucketId)
            schemas = bucket.get_schemas_info(False)
            schemaTypes = [schema['schemaType'] for schema in schemas['data']]
            if schemaType in schemaTypes:
                self.write_and_set_status(
                    {MESSAGE: "A schema with the schemaType '{0}' is already bound to the bucket.".format(schemaType)},
                    HTTPStatus.FORBIDDEN)
                return

            # check if schema with the same has as the current annotation exist:
            targetType = TargetType(targetTypeName)
            nestedFields = []
            if targetType == TargetType.document_surface1d:
                nestedFields.append("offsets")
            schemaId = get_schema_list(envId, authorization).add_json_schema_as_hash(body, False, nestedFields)
            bucket.add_or_update_schema_to_bucket(schemaId, schemaType, targetType, {})

            self.write_and_set_status(None, HTTPStatus.NO_CONTENT)
        except CorpusNotFoundException as err:
            self.write_and_set_status({MESSAGE: "Corpus does not exist.Extra info: '{0}'".format(err)},
                                      HTTPStatus.UNPROCESSABLE_ENTITY)
        except BucketNotFoundException as err:
            self.write_and_set_status({MESSAGE: "Bucket does not exist.Extra info: '{0}'".format(err)},
                                      HTTPStatus.UNPROCESSABLE_ENTITY)
        except SchemaBindingInvalid as err:
            self.write_and_set_status({MESSAGE: "Schema Binding Invalid: '{0}'".format(err)},
                                      HTTPStatus.UNPROCESSABLE_ENTITY)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)

    def options(self, corpusId, bucketId):
        self.write_and_set_status(None, HTTPStatus.OK)

    def put(self, corpusId, bucketId):
        try:
            body = self.strip_body_bom()
            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)

            if is_missing_required_fields(body, ["targetType", "schemaType", "properties"]):
                self.write_and_set_status(
                    {MESSAGE: missing_fields_message(body, ["targetType", "schemaType", "properties"])},
                    HTTPStatus.UNPROCESSABLE_ENTITY)
                return

            schemaType = body["schemaType"]
            targetTypeName = body["targetType"]
            if not TargetType.has(targetTypeName):
                self.write_and_set_status({MESSAGE: "Target type {0} not supported".format(targetTypeName)},
                                          HTTPStatus.UNPROCESSABLE_ENTITY)
                return

            # Is there currently a schema of schemaType associated with the bucket?
            bucket = get_master_document_corpus_list(envId, authorization).get_corpus(corpusId).get_bucket(bucketId)
            schemas = bucket.get_schemas_info(False)
            schemaTypes = [schema['schemaType'] for schema in schemas['data']]
            if schemaType not in schemaTypes:
                self.write_and_set_status(
                    {MESSAGE: "There is no schema with the schemaType '{0}' currently bound to the bucket.".format(schemaType)},
                    HTTPStatus.NOT_FOUND)
                return

            # check if schema with the same has as the current annotation exist:
            targetType = TargetType(targetTypeName)
            nestedFields = []
            if targetType == TargetType.document_surface1d:
                nestedFields.append("offsets")
            schemaId = get_schema_list(envId, authorization).add_json_schema_as_hash(body, False, nestedFields)
            bucket.add_or_update_schema_to_bucket(schemaId, schemaType, targetType, {})

            self.write_and_set_status(None, HTTPStatus.NO_CONTENT)
        except EsSchemaMigrationInvalidException as err:
            self.write_and_set_status({MESSAGE:
                "Cannot update schema because changes are not compatible with document in old schema.Extra info: '{0}'".format(
                    err)},
                HTTPStatus.UNPROCESSABLE_ENTITY)
        except EsSchemaMigrationDeleteFieldsNotSupportedException as err:
            self.write_and_set_status(
                {MESSAGE: "Can not delete fields from existing schema.Missing Fields: '{0}'".format(err)},
                HTTPStatus.UNPROCESSABLE_ENTITY)
        except CorpusNotFoundException as err:
            self.write_and_set_status({MESSAGE: "Corpus does not exist.Extra info: '{0}'".format(err)},
                                      HTTPStatus.UNPROCESSABLE_ENTITY)
        except BucketNotFoundException as err:
            self.write_and_set_status({MESSAGE: "Bucket does not exist.Extra info: '{0}'".format(err)},
                                      HTTPStatus.UNPROCESSABLE_ENTITY)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)
