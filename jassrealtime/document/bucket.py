# coding: utf-8

# A bucket contains all annotations.

import logging
import uuid
from enum import Enum, unique
from elasticsearch_dsl import Search, Q

from ..core.document_directory import *
from ..core.master_factory_list import get_master_document_directory_list, get_schema_list
from ..core.esutils import multi_indexes_small_search
from ..core.settings_utils import get_number_of_replicas, get_number_of_shards

BUCKET_BINDING_INDEX_MAPPING = {
    "mappings": {
        "default": {
            "properties": {
                "jsonSchemaId": {
                    "type": "keyword",
                },
                "jsonSchemaHash": {
                    "type": "keyword",
                },
                "esHash": {
                    "type": "keyword",
                },
                "docType": {
                    "type": "keyword",
                },
                "bucketId": {
                    "type": "keyword",
                },
                "corpusId": {
                    "type": "keyword",
                },
                "targetType": {"type": "keyword"},
                "target": {"type": "keyword", "index": "false"},

            }
        }
    }
}

BUCKET_MAPPING = {
    "mappings": {
        "default": {
            "properties": {
                "name": {"type": "text"},
                "corpusId": {"type": "keyword"},
                "bucketId": {"type": "keyword"},
            }
        }
    }
}


class BucketException(Exception):
    pass


class BucketAlreadyExistsException(BucketException):
    pass


class BucketNotFoundException(BucketException):
    pass


class TargetNotSupportedException(BucketException):
    pass


class InvalidSearchParameterException(BucketException):
    pass


class InvalidCharactersInBucketId(BucketException):
    pass


class SchemaTypeNotFoundException(BucketException):
    pass


@unique
class TargetType(Enum):
    document_surface1d = "document_surface1d"
    document = "document"
    corpus = "corpus"

    @staticmethod
    def has(name: str):
        return name in TargetType.__members__


class BucketList:
    @staticmethod
    def create(envId: str, sett: dict, authorization: BaseAuthorization):
        """
        Creates bucket directory and bindings

        :return:
        """
        es = get_es_conn()
        bucketBindingIndex = envId + sett['CLASS_PREFIX'] + sett['BUCKET_BINDING_INDEX_SUFFIX']
        if check_index_name_valid_for_create(bucketBindingIndex, envId, sett['CLASS_PREFIX']):
            es_wait_ready()
            body = BUCKET_BINDING_INDEX_MAPPING
            body["settings"] = {}
            body["settings"]["index"] = {"number_of_shards": get_number_of_shards(),
                                         "number_of_replicas": get_number_of_replicas()}
            es.indices.create(index=bucketBindingIndex, body=body)
            es_wait_ready()
            masterList = get_master_document_directory_list(envId, authorization)
            bucketMasterDir = masterList.create_document_directory(sett['MASTER_BUCKET_ID'])
            es_wait_ready()
            time.sleep(1)  # time to create indices
            bucketMasterDir.add_or_update_schema(BUCKET_MAPPING['mappings']['default'])
            es_wait_ready()
            time.sleep(1)  # time to create indices

        return BucketList(envId, sett, bucketMasterDir, authorization)

    def __init__(self, envId, sett, dd: DocumentsDirectory, authorization: BaseAuthorization):

        self.envId = envId
        self.classPrefix = sett['CLASS_PREFIX']
        self.masterBucketId = sett['MASTER_BUCKET_ID']
        self.bucketBindingIndex = envId + sett['CLASS_PREFIX'] + sett['BUCKET_BINDING_INDEX_SUFFIX']
        self.masterList = get_master_document_directory_list(envId, authorization)
        self.dd = self.masterList.get_directory(self.masterBucketId)
        self.authorization = authorization

    def delete(self):
        """
        Delete bucket directory and all bindings. Must have authorisation to delete each bucket.
        :return:
        """
        es = get_es_conn()
        es_wait_ready()
        bucketIDs = self.dd.small_search(useScan=False)
        for metadata in bucketIDs:
            self.delete_bucket(metadata["corpusId"], metadata["id"])

        # delete_indices(self.)
        self.masterList.delete_document_directory(self.dd)
        es_wait_ready()
        delete_indices(self.bucketBindingIndex, self.envId, self.classPrefix)

    @staticmethod
    def get_bucket_corpus_id(corpusId: str, bucketId: str) -> str:
        """
        Creates an id with combined bucketId and corpusId
        :param id:
        :param corpusId:
        :return:
        """
        return corpusId + "_" + bucketId

    def create_bucket(self, name: str, corpusId: str, id: str = None):
        """

        :param authorization:           User authorization object
        :param name:
        :param corpusId:                ID of an existing corpus (Not checked)
        :param id:                      ID for the bucket.  Should be alphanumeric _- lowercase. If none it will generate one automatically.
        :return:
        """
        logger = logging.getLogger(__name__)

        self.authorization.can_create_bucket(corpusId)
        if id:
            bucketCorpusId = BucketList.get_bucket_corpus_id(corpusId, id)
            if self.dd.document_exist(bucketCorpusId):
                logger.info("Bucket already exists with the following id:".format(bucketCorpusId))
                raise BucketAlreadyExistsException(id)
        else:
            id = gen_uuid()
            bucketCorpusId = BucketList.get_bucket_corpus_id(corpusId, id)

        self.dd.add_document(
            {"name": name, "corpusId": corpusId, "bucketId": id}, bucketCorpusId)
        dd = self.masterList.create_document_directory(bucketCorpusId, None, False)

        return Bucket(self.envId, self.bucketBindingIndex, self.authorization, dd, name, id, corpusId)

    def get_bucket(self, corpusId: str, id: str) -> 'Bucket':
        """
        Gets the subbucket.

        :param autorization:        Authorization object of current accessing user.
        :param id: Id of the bucket.
        :return:
        """

        self.authorization.can_read_bucket(corpusId, id)

        try:
            bucketCorpusId = BucketList.get_bucket_corpus_id(corpusId, id)
            bucketInfo = self.dd.get_document(bucketCorpusId)
            dd = self.masterList.get_directory(bucketCorpusId)
            return Bucket(self.envId, self.bucketBindingIndex, self.authorization, dd, bucketInfo["name"], id,
                          bucketInfo["corpusId"])
        except DocumentNotFoundException:
            raise BucketNotFoundException()

    def delete_bucket(self, corpusId, id):
        """
        Delets a bucket and all its annotations. This function will fail if there is more than 10 000 schema bindings

        :param authorization:   User authorization object
        :param corpusId:        id of the corpus from containing this bucket
        :param id:     id of the bucket.
        :return:
        """
        logger = logging.getLogger(__name__)
        self.authorization.can_delete_bucket(corpusId, id)
        try:
            bucket = self.get_bucket(corpusId, id)
            # delete all bucket bindings
            es = get_es_conn()
            schemasBindings = multi_indexes_small_search(self.bucketBindingIndex, {},
                                                         {"corpusId": corpusId, "bucketId": id}, None, {}, {}, False)
            for binding in schemasBindings:
                es.delete(index=self.bucketBindingIndex, doc_type="default", id=binding["id"])

            bucketCorpusId = BucketList.get_bucket_corpus_id(corpusId, id)
            self.dd.delete_document(bucketCorpusId)
            self.masterList.delete_document_directory(bucket.dd)

        except exceptions.NotFoundError as e:
            logger.warning(e)
            raise BucketNotFoundException
        except DocumentDirectoryDoesntExistsException as e:
            logger.warning(e)
            raise BucketNotFoundException

    def get_all_buckets_for_corpus(self, corpusId: str) -> List:
        """
        Returns all buckets for a corpus the user is authorised to see.

        :param authorization:  User authorization object
        :param corpusId:       The id of the corpus
        :return:               List of buckets
        """
        res = []
        bucketInfoArr = self.dd.small_search(termFields={"corpusId": corpusId}, useScan=False)
        for bucketInfo in bucketInfoArr:
            try:
                self.authorization.can_read_bucket(corpusId, bucketInfo["bucketId"])
                bucketId = bucketInfo["bucketId"]
                bucketCorpusId = BucketList.get_bucket_corpus_id(corpusId, bucketId)
                dd = self.masterList.get_directory(bucketCorpusId)
                res.append(
                    Bucket(self.envId, self.bucketBindingIndex, self.authorization, dd, bucketInfo["name"], bucketId,
                           bucketInfo["corpusId"]))
            finally:
                # If no access we simply dont show it.
                pass

        return res

    def get_bucket_indexes(self, corpusId, bucketsIds: List[str] = [], bucketNames: List[str] = [],
                           docTypes=["default"]):
        """
        Internal method to get indexes to search from

        :param bucketsIds:
        :param bucketNames:
        :param docTypes:        Get indexes for specific docTypes. If empty will use all docTypes.
        :return: {searchIndices: "<all indexes separted by comma>", indexByBucketId : {index1 : bucketId1, index2 : bucketId1 ...}}
        """
        logger = logging.getLogger(__name__)

        es = get_es_conn()
        s = Search(using=es, index=self.bucketBindingIndex)
        terms = {}

        if not corpusId:
            logger.info("Invalid search corpusId: '{0}'".format(corpusId))
            raise InvalidSearchParameterException("Invalid search corpusId: '{0}'".format(corpusId))
        else:
            terms["corpusId"] = corpusId

        # Filter Ids
        if bucketsIds:
            for id in bucketsIds:
                if "*" in id:
                    logger.info("Invalid search bucketIds: {0}".format(id))
                    raise InvalidSearchParameterException("Invalid search bucketIds: {0}".format(id))

        # find all matching buckets.
        if bucketNames and bucketsIds:
            s.query = Q('bool', must=[Q('term', corpusId=terms["corpusId"])]
                        , should=[Q("term", bucketId=bucketsIds), Q("match", name=bucketNames)]
                        , minimum_should_match=1)
        elif bucketNames:
            s.query = Q('bool', must=[Q('term', corpusId=terms["corpusId"])]
                        , should=[Q("match", name=bucketNames)], minimum_should_match=1)
        elif bucketsIds:
            s.query = Q('bool', must=[Q('term', corpusId=terms["corpusId"])]
                        , should=[Q("term", bucketId=bucketsIds)], minimum_should_match=1)
        else:
            s.query = Q('bool', must=[Q('term', corpusId=terms["corpusId"])])

        bucketInfo = s.execute()
        indexByBucketId = {}
        searchIndices = []

        # TODO: exception for bucket not allowed.
        for info in bucketInfo:
            bucket = self.get_bucket(corpusId, info.meta.id)
            strIndexes = bucket.dd.get_indices(docTypes)
            searchIndices.append(strIndexes)
            indices = strIndexes.replace('*', "").split(',')
            for index in indices:
                indexByBucketId[index] = bucket.id

        return {"searchIndices": searchIndices.join(","), "indexByBucketId": indexByBucketId}


class Bucket:
    """
    Creates a document bucket.

    """

    def __init__(self, envId, bucketBindingIndex: str, authorization: BaseAuthorization, dd: DocumentsDirectory,
                 name, id: str, corpusId: str):
        self.envId = envId
        self.authorization = authorization
        self.dd = dd
        self.name = name
        self.id = id
        self.corpusId = corpusId
        self.bucketBindingIndex = bucketBindingIndex

    def get_binding_id(self, docType: str):
        """
        Returns a unique binding id used to bind a schema to a doctype
        :param : Doc type

        :return:
        """
        return string_to_id(self.corpusId + self.id) + string_to_id(docType)

    def add_or_update_schema_to_bucket(self, jsonSchemaId: str, docType: str, targetType: TargetType, target: str):
        """

        :param jsonSchemaId:    Id of an existing json schema
        :param docType:         Type of document to which to add a schema.
        :param targetType:              Target type for this bucket
        :param target:                  Contains information pertaining to target depends on the target
        :return:
        """

        self.authorization.can_add_json_schema_to_bucket(self.corpusId, self.id)
        if not isinstance(targetType, TargetType):
            raise TargetNotSupportedException

        es = get_es_conn()
        id = self.get_binding_id(docType)
        schemaList = get_schema_list(self.envId, self.authorization)
        schemaInfo = schemaList.get_json_schema_info(jsonSchemaId)
        jsonSchemaHash = schemaInfo["jsonSchemaHash"]
        esHashTo = schemaInfo["esHash"]
        bindingInfo = {"jsonSchemaId": jsonSchemaId, "jsonSchemaHash": jsonSchemaHash, "esHash": esHashTo,
                       "docType": docType, "bucketId": self.id, "corpusId": self.corpusId,
                       "targetType": targetType.name, "target": json.dumps(target)}
        # ATTEMPT to add the schema to document directory
        try:
            res = es.get(index=self.bucketBindingIndex, doc_type="default", id=id)
            # an old schema already exist.
            esHashFrom = res["_source"]["esHash"]
            deltaProperties = schemaList.calculate_properties_delta(
                schemaList.get_es_properties(esHashFrom),
                schemaList.get_es_properties(esHashTo)
            )
            if deltaProperties:
                self.dd.add_or_update_schema(deltaProperties, docType)
                es.update(index=self.bucketBindingIndex, doc_type="default", id=id, body={"doc": bindingInfo})
        except exceptions.NotFoundError:
            es.create(index=self.bucketBindingIndex, doc_type="default", id=id, body=bindingInfo)
            self.dd.add_or_update_schema(schemaList.get_es_properties(esHashTo), docType)

    def delete_schema_type(self, docType: str):
        """
        Deletes a schema type and all its associated annotations

        :param docType: Schema to delete
        :return:
        """
        es = get_es_conn()
        logger = logging.getLogger(__name__)
        id = self.get_binding_id(docType)
        try:
            res = es.get(index=self.bucketBindingIndex, doc_type="default", id=id)
            try:
                self.dd.delete_doc_type(docType)
            except Exception as e:
                logger.error("Failed to delete schema type index: " + str(e))

            es.delete(index=self.bucketBindingIndex, doc_type="default", id=id)

        except exceptions.NotFoundError:
            raise SchemaTypeNotFoundException("Schema to delete not found {0}".format(docType))

    def get_schemas_info(self, includeJson=False, useScan=False):
        """
        Returns the docType to which the schema is associated to for all schemas associated to the bucket.

        :param includeJson: If true will also return json contents
        :param includeJson: If True will use scan api to return schemas.
            Should only be True if there is more than 10 000 schemas in a bucket.
        :return:    {data: [{"schemaType":"type of schema","jsonSchema":"json of schema if applicable"}]}
        """
        result = {"data": []}
        schemasBindings = multi_indexes_small_search(self.bucketBindingIndex, {},
                                                     {"corpusId": self.corpusId, "bucketId": self.id},
                                                     ["jsonSchemaId", "docType"], {}, {}, useScan)
        if includeJson:
            schemasList = get_schema_list(self.envId, self.authorization)
            for binding in schemasBindings:
                schemaInfo = schemasList.get_json_schema_info(binding["jsonSchemaId"])
                result["data"].append(
                    {"schemaType": binding["docType"], "jsonSchema": json.dumps(schemaInfo["jsonSchema"])})
        else:
            for binding in schemasBindings:
                result["data"].append({"schemaType": binding["docType"]})

        return result

    def delete_annotations(self, schemaType: string):
        """
        Deletes all annotations with a given schema
        :return:
        """
        self.dd.empty_doc_type(schemaType)

    def add_annotations_mini_batch(self, jsonAnnotationsWithIds: List, docType: str = "default",
                                   shouldValidate: bool = False):
        """
        Add a batch of annotations. TERRIBLY INEFFICIENT.
        If one annotation crashes, it stops in the middle of operation.

        :param jsonAnnotationWithIds:  Format : [anno1,annoN] with their ids.
                    (if no ids present they will be generated)
        :param docType:
        :param shouldValidate:
        :return:
        """
        for annotation in jsonAnnotationsWithIds:
            annotationId = None
            if "id" in annotation:
                annotationId = annotation["id"]
                del annotation[id]

            self.add_annotation(annotation, docType, annotationId, shouldValidate)

    def get_es_index_per_schema_type(self):
        """
        For each docType return the associated elastic search index.

        :return: {<schemaType>:<index>}
        """
        return self.dd.get_indices_per_doc_type()

    def add_annotation(self, jsonAnnotation: dict, docType: str = "default", annotationId: str = None,
                       shouldValidate: bool = False):
        """
        Creates a new annotation. Will return an exception if annotation already exists.

        :param docType:
        :param jsonAnnotation:  Python object representing an annotation
        :param annotationId:
        :param shouldValidate:  If true will calls validation function, to validate annotation content
        :return:
        """
        self.authorization.can_add_annotation(self.corpusId, self.id)
        if not annotationId:
            annotationId = gen_uuid()

        if shouldValidate:
            self.validate_annotation()
            raise NotImplementedError()
            # TODO  verify that annotation respect schema

        return self.dd.add_document(jsonAnnotation, annotationId, docType)

    def get_annotation(self, id: str, docType: str = "default"):
        """
        Returns an annotation. Throws exception if not found

        :param id:
        :return:
        """
        return self.dd.get_document(id, docType)

    def update_annotation(self, jsonAnnotation: dict, docType: str = "default", annotationId: str = None,
                          shouldValidate: bool = False):
        """
        Updates an annotation. If annotation does not exists will throw an exception.

        :param docType:
        :param jsonAnnotation:  Python object representing an annotation
        :param annotationId:
        :param shouldValidate:  If true will calls validation function, to validate annotation content.
        :return:
        """

        self.authorization.can_update_annotation(self.corpusId, self.id)

        if shouldValidate:
            raise NotImplementedError()
            # TODO  verify that annotation respect schema

        self.dd.update_document(jsonAnnotation, annotationId, docType)

    def delete_annotation(self, annotationId: str, docType: str = "default"):
        """
        Deletes an annotation. Will throw an

        :param docType:
        :param annotationId:
        :return:
        """

        self.dd.delete_document(annotationId, docType)
