# coding: utf-8

from elasticsearch import helpers, exceptions
from elasticsearch_dsl import Search, Q
import time
import traceback
from typing import List, Dict

from jassrealtime.core.utils import gen_uuid
from .schema_list import *
from .esutils import *
from .settings_utils import get_scan_scroll_duration, get_number_of_replicas, get_number_of_shards, \
    get_nb_documents_per_scan_scroll

ANALYSIS_FILTER = {
    "ngram_filter": {
        "type": "nGram",
        "min_gram": 2,
        "max_gram": 3
    }
}

ANALYSIS_TOKENIZER = {
    "path_tokenizer": {
        "type": "path_hierarchy",
        "delimiter": "/",
        "skip": 0
    },
    "autocomplete": {
        "type": "edge_ngram",
        "min_gram": 2,
        "max_gram": 10,
        "token_chars": [
            "letter", "digit"
        ]
    }
}

ANALYSIS_ANALYZER = {
    "ngram_filter_analyzer": {
        "type": "custom",
        "tokenizer": "standard",
        "filter": [
            "lowercase",
            "ngram_filter"
        ]
    },
    "autocomplete": {
        "tokenizer": "autocomplete",
        "filter": [
            "lowercase"
        ]
    },
    "autocomplete_search": {
        "tokenizer": "lowercase"
    },
    "path_analyzer": {
        "tokenizer": "path_tokenizer"
    }
}

ANALYSIS = {
    "analyzer": ANALYSIS_ANALYZER,
    "filter": ANALYSIS_FILTER,
    "tokenizer": ANALYSIS_TOKENIZER
}

MAX_RESULT_WINDOW = 500000


class DocumentDirectoryException(Exception):
    pass


class DocumentDirectoryAlreadyExistsException(DocumentDirectoryException):
    pass


class DocumentDirectoryAliasAlreadyExistsException(DocumentDirectoryException):
    pass


class DocumentDirectoryDoesntExistsException(DocumentDirectoryException):
    pass


class DocumentDirectoryFailedToDelete(DocumentDirectoryException):
    pass


class DocumentNotFoundException(DocumentDirectoryException):
    def __init__(self, document_id):
        self.document_id = document_id


class DocumentAlreadyExistsException(DocumentDirectoryException):
    pass


class DocumentDirectoryNoSchemaFoundException(DocumentDirectoryException):
    pass


class NoFieldPassedToSearchException(DocumentDirectoryException):
    pass


class DocumentDoesNotRespectSchemaException(DocumentDirectoryException):
    pass


class CriticalDeleteIndexFailIndexNameTooShort(DocumentDirectoryException):
    """
    This is a special exception, used to notify the user that attempted to delete an index
    by not passing enough parameters. Useful in preventing unlucky delete *
    """
    pass


class DocumentDirectoryList:
    """
    Responsible for managing a list of document directories. Contains a list of all document directories
    for a particular envId.
    """

    def __init__(self, envId: str, sett: dict, authorization: BaseAuthorization):
        """
        Creates a dictionary for a particular envId.
        PRECONDITION: envId must be at least 3 characters long.

        :param envId: ALPHANUMERIC (With underscores) string representing a envId.
        :param sett:  Settings object containing specific settings.
        """
        self.envId = envId
        self.authorization = authorization
        self.classPrefix = sett['CLASS_PREFIX']
        self.indexDataSuffix = sett['INDEX_DATA_SUFFIX']  # Suffix to indicate index containing data
        self.indexTypeSuffix = sett['INDEX_TYPE_SUFFIX']  # Suffix to indicate index containing type mappings
        self.indexDefaultTypeSuffix = sett[
            'INDEX_DEFAULT_TYPE_SUFFIX']  # Document without type will be put to this index.
        self.defaultType = sett['DEFAULT_TYPE']  # Type for documents without type. DO NOT CHANGE THIS VALUE
        self.directoryDocType = sett['DIRECTORY_DOC_TYPE']  # Type of DocumentDirectory objects inside master index
        self.masterDirectoryIndex = envId + self.classPrefix + sett[
            'MASTER_DIRECTORY_INDEX_SUFFIX']  # Index to list all directories

    @staticmethod
    def create(envId, sett: dict, authorization: BaseAuthorization):
        """
        This function will creates a new directory list for a given envId.
        Will throw an exception a document directory already exists.

        :return:    DocumentDirectoryList object.
        """
        logger = logging.getLogger(__name__)
        authorization.can_create_env()

        masterList = DocumentDirectoryList(envId, sett, authorization)

        es = get_es_conn()
        body = {"settings": {}}
        body["settings"]["index"] = {"number_of_shards": get_number_of_shards(),
                                     "number_of_replicas": get_number_of_replicas()}
        if check_index_name_valid_for_create(masterList.masterDirectoryIndex, masterList.envId, ""):
            es.indices.create(masterList.masterDirectoryIndex, body=body)
        else:
            logger.log("Invalid master directory name {0}".format(masterList.masterDirectoryIndex))
            raise DocumentDirectoryException(
                "Invalid master directory name {0}".format(masterList.masterDirectoryIndex))
        return masterList

    def delete(self):
        """
        Deletes all directories listed in directory listing.
        Be careful as it actually wipes the whole bd.

        :return:
        """
        logger = logging.getLogger(__name__)
        self.authorization.can_delete_env()
        if check_indices_name_valid_for_delete(self.masterDirectoryIndex, self.envId, self.classPrefix):
            directoriesList = self.get_directories()
            for dd in directoriesList:
                self.delete_document_directory(dd)

            es = get_es_conn()
            es_wait_ready()
            delete_indices(self.masterDirectoryIndex, self.envId, self.classPrefix)
        else:
            logger.log("Invalid master directory name to delete {0}".format(self.masterDirectoryIndex))
            raise DocumentDirectoryException(
                "Invalid master directory name to delete {0}".format(self.masterDirectoryIndex))

            # TODO delete rest

    def get_directories(self):
        """
        Returns a list of all directories in the system

        :return:
        """
        es = get_es_conn()
        query = {"query": {"match_all": {}}}

        es_wait_ready()
        esresult = helpers.scan(es, query=query, doc_type=self.directoryDocType, index=self.masterDirectoryIndex,
                                scroll=get_scan_scroll_duration(), size=get_nb_documents_per_scan_scroll())
        directoryList = []
        for directory in esresult:
            source = directory["_source"]
            alias = None
            if "_alias" in directory:
                alias = directory["_alias"]

            dd = DocumentsDirectory(directory["_id"], alias, self)
            directoryList.append(dd)

        return directoryList

    def create_document_directory(self, id: str, alias=None, addDefaultType=True):
        """
        Creates a new document directory

        :param id: Searchable not empty lowercase alphanumeric name of the directory.
            Must be unique in the system.
        :param alias: TODO. Alias needs to be unique in the system to enable search by alias.
            If no alias needed i should be set to None or empty.
        :param addDefaultType: if true a "default" type will be added when the document is created

        :return DocumentDirectory created
        """

        logger = logging.getLogger(__name__)
        es = get_es_conn()

        directoryExists = True
        try:
            es_wait_ready()
            directoryExists = es.get(index=self.masterDirectoryIndex, doc_type=self.directoryDocType, id=id)
        except exceptions.NotFoundError:
            directoryExists = False

        # prevent trolls from creating a directory with the same id as master directory.
        if id == self.masterDirectoryIndex:
            directoryExists = True

        if directoryExists:
            logger.warning("DocumentDirectory '{0}' already exists".format(id))
            raise DocumentDirectoryAlreadyExistsException(id)

        if alias and not alias.isspace():
            es_wait_ready()
            aliasQuery = es.search(index=self.masterDirectoryIndex, doc_type=self.directoryDocType,
                                   body={"query": {"match": {"alias": alias}}})
            if len(aliasQuery["hits"]["hits"]) > 0:
                logger.warning("Can not create document '{0}, alias '{1}' already exists'".format(alias, id))
                raise DocumentDirectoryAliasAlreadyExistsException(alias)
            else:
                body = {"id": id, "alias": alias}
        else:
            body = {"id": id}
            alias = None

        es_wait_ready()
        setting_body = {"settings": {}}
        setting_body["settings"]["index"] = {"number_of_shards": get_number_of_shards(),
                                             "number_of_replicas": get_number_of_replicas()}
        if addDefaultType:
            defaultDataIndex = self.envId + self.classPrefix + id + self.indexDataSuffix + self.indexDefaultTypeSuffix
            typeIndex = self.envId + self.classPrefix + id + self.indexTypeSuffix
            es.create(index=self.masterDirectoryIndex, doc_type=self.directoryDocType, id=id, body=body)
            if check_index_name_valid_for_create(defaultDataIndex, self.envId, self.classPrefix) and \
                    check_index_name_valid_for_create(typeIndex, self.envId, self.classPrefix):
                es_wait_ready()
                es.indices.create(defaultDataIndex, body=setting_body)
                es_wait_ready()
                es.indices.create(typeIndex, body=setting_body)
            else:
                logger.error("One of the indexes is invalid {0},{1},{2}".format(id, defaultDataIndex, typeIndex))
                raise ESInvalidIndexName(
                    "One of the indexes is invalid {0},{1},{2}".format(id, defaultDataIndex, typeIndex))
            # wait 1 second in order to make sure that document directory is well created
            time.sleep(1)
            es.create(index=typeIndex, doc_type="directory_type", id="default", body={"indexName": defaultDataIndex})
        else:
            typeIndex = self.envId + self.classPrefix + id + self.indexTypeSuffix
            es.create(index=self.masterDirectoryIndex, doc_type=self.directoryDocType, id=id, body=body)
            if check_index_name_valid_for_create(typeIndex, self.envId, self.classPrefix):
                es_wait_ready()
                es.indices.create(typeIndex, body=setting_body)
            else:
                logger.error("One of the indexes is invalid {0},{1}".format(id, typeIndex))
                raise ESInvalidIndexName(
                    "One of the indexes is invalid {0},{1}".format(id, typeIndex))
        if alias:
            logger.info("Document '{0}' with alias {1} created".format(id, alias))
        else:
            logger.info("Document '{0}' created without alias".format(alias, id))

        return DocumentsDirectory(id, alias, self)

    def delete_document_directory(self, dd):
        """
        Deletes a document directory and all its associated indices
        :return:
        """
        es = get_es_conn()
        errorMessage = ""
        # delete from master index
        try:
            es_wait_ready()
            es.delete(index=self.masterDirectoryIndex, doc_type=self.directoryDocType, id=dd.id)
        except:
            errorMessage = errorMessage + traceback.format_exc()

        try:
            es_wait_ready()
            delete_indices(dd.dataIndexPrefix + "*", self.envId, self.classPrefix)
        except:
            errorMessage = errorMessage + traceback.format_exc()

        # There is only 1 type index
        try:
            es_wait_ready()
            delete_indices(dd.typeIndex, self.envId, self.classPrefix)
        except:
            errorMessage = errorMessage + traceback.format_exc()

        if errorMessage:
            raise DocumentDirectoryFailedToDelete(errorMessage)

    def get_directory(self, id: str):
        """
        Returns a document directory object associatged with directoryName.
        Throwsn an exception if not found.

        :param id:              Unique Id of the directory.
        :return:
        """
        es = get_es_conn()
        esres = {}
        try:
            esres = es.get(index=self.masterDirectoryIndex, doc_type=self.directoryDocType, id=id)
        except exceptions.NotFoundError:
            raise DocumentDirectoryDoesntExistsException()

        # TODO Add Alias.
        return DocumentsDirectory(id, None, self)


def remove_non_settable_fields(fields: List[str], collection: Dict[str, str]) -> None:
    for field in fields:
        if field in collection:
            del collection[field]


class DocumentsDirectory:
    """
    This class represents a virtual directory of documents. A document in this context is any json object
    with one hierarchy.

    This is an internal class, without authroisation / authentification and should not be accessed directly.

    This class will automatically index any field for the document, so the document could be searched.

    Documents in a directory can be grouped by type, sharing the same schema.
    The schema can be updated dynamically, if the existing objects can be automatically adapted to the new schema.
      * adding new optional fields will work.
      * removing or deleting will not.

    """

    def __init__(self, id: str, alias: str, masterList: DocumentDirectoryList):
        """
        :param id: Id of the directory
        :param alias: Alias referencing a document.
        """
        self.masterList = masterList
        self.id = id
        self.alias = alias
        self.typeIndex = masterList.envId + masterList.classPrefix + id + masterList.indexTypeSuffix
        self.dataIndexPrefix = masterList.envId + masterList.classPrefix + id + masterList.indexDataSuffix
        self.frenchIndexPrefix = self.dataIndexPrefix + "_FR"
        self.englishIndexPrefix = self.dataIndexPrefix + "_EN"
        self.defaultDataIndex = masterList.envId + masterList.classPrefix + id + masterList.indexDataSuffix + masterList.indexDefaultTypeSuffix

    def document_exist(self, id, docType="default"):
        """
        Returns true if the document with a give id exists

        :param id:         Id of the document
        :param docType:    Type of the document.If not supplied will use default type.
        :return:
        """

        es = get_es_conn()

        indexName = None
        try:
            if docType == self.masterList.defaultType:
                indexName = self.defaultDataIndex
                doc = es.get(index=indexName, id=id, doc_type=docType)
            elif docType == None:
                indexName = self.dataIndexPrefix + "*"
                s = Search(using=es, index=indexName).query("term", _id=id)
                res = s.execute()
                return (len(res.hits.hits) > 0)
            else:
                res = es.get(index=self.typeIndex, doc_type="directory_type", id=docType)
                indexName = res["_source"]["indexName"]
                doc = es.get(index=indexName, id=id, doc_type=docType)

            return True
        except exceptions.NotFoundError:
            return False

    def get_indices(self, docTypes: List = ["default"]) -> str:
        """
        Returns a list of all indexes for the given doc types.

        :param docTypes:        List of Doctypes to search, if empty will search all docTypes
        :return:                A string representing indexes to search. (will use * to regroup multiple indices)
        """

        es = get_es_conn()

        indexNamesStr = ""
        if docTypes:
            s = Search(using=es, index=self.typeIndex, doc_type="directory_type").query("ids", values=docTypes)
            s = s.params(scroll=get_scan_scroll_duration(), size=get_nb_documents_per_scan_scroll())

            indexNamesQuery = s.source(["indexName"])
            indexNamesArr = []
            for indexNamePart in indexNamesQuery.scan():
                indexNamesArr.append(indexNamePart["indexName"])
            indexNamesStr = ','.join(indexNamesArr)
        else:
            indexNamesStr = self.dataIndexPrefix + "*"

        return indexNamesStr

    def get_indices_per_doc_type(self):
        """
        Return a list of es indexes per doc type.
        :return: {<docType1> ; <index1>, ...}.
        """
        es = get_es_conn()

        s = Search(using=es, index=self.typeIndex, doc_type="directory_type")
        s = s.params(scroll=get_scan_scroll_duration(), size=get_nb_documents_per_scan_scroll())
        indexNamesQuery = s.source(["indexName"])
        indicesPerDocType = {}
        for res in indexNamesQuery.scan():
            indicesPerDocType[res.meta.id] = res["indexName"]

        return indicesPerDocType

    def empty_doc_type(self, docType: str):
        """
        Removes all documents for a specific doc type.
        :param docType:
        :return:
        """

        es = get_es_conn()
        es_wait_ready()
        indices = self.get_indices_per_doc_type()
        if docType not in indices:
            raise DocumentDirectoryNoSchemaFoundException("{0} not found.".format(docType))

        # remove indices
        index = indices[docType]
        # get index settings
        settings = es.indices.get_settings(index=index)
        mappings = es.indices.get_mapping(index=index, doc_type=docType)
        body = {"settings": settings[index]['settings'], "mappings": mappings[index]["mappings"]}

        # delete index
        self.delete_doc_type(docType)

        es_wait_ready()

        # recreate index
        # Remove non settable fields (if they are present, it causes an unknown setting exception)
        remove_non_settable_fields(["creation_date", "provided_name", "uuid", "version"], body['settings']['index'])
        es.indices.create(index, body=body)

        # remap index.
        es.create(index=self.typeIndex, doc_type="directory_type", id=docType, body={"indexName": index})

    def delete_doc_type(self, docType):
        """
        Removes doc type and all its associated documents
        :param docType:
        :return:
        """

        es = get_es_conn()

        indices = self.get_indices_per_doc_type()
        if docType not in indices:
            raise DocumentDirectoryNoSchemaFoundException("{0} not found.".format(docType))

        # remove indices
        index = indices[docType]
        delete_indices(index, self.masterList.envId, self.masterList.classPrefix)

        # remove index from the type reference,
        es.delete(index=self.typeIndex, doc_type="directory_type", id=docType)

    def small_search(self, docTypes: List = ["default"], matchFields: dict = {}, termFields: dict = {},
                     returnFields: List = None, filterMatch={}, filterTerms={}, useScan=True):
        """
        Search used to retrieve a reasonable amount of documents (All docs stored in memory, no streaming).

        :param docTypes:        List of Doctypes to search, if empty will search all docTypes
        :param matchFields:     Uses match in elastic search. (This will use any analysers associated with the field).
                                    (Ie if you search for value "car", and a document with value "Red car" exists. It will return it.
        :param termFields:      Uses term in elastic search. Exact term must exist.
                                    (Ie if you search for value "car", and a document with value "Red car" exists. It will NOT return it.
        :param returnFields:    Returns the fields specified in the list. If None, return all fields. It will always return type and id,
                                    independent on filters.
        :param filterMatch:     Use match in elastic search filter context. Exact term must exist. Quicker than matchFields, but doesnt use a score.
        :param filterTerms:     Use term in elastic search filter context. Exact term must exist. Quicker than termFields, but doesnt use a score.
        :param useScan:         If True, uses scroll API to return all results. Howhever only 1 scroll can exist per index currently.
            If false uses search api, which returns at most 10 0000 results.
        :return:
        """

        es = get_es_conn()

        indexNamesStr = self.get_indices(docTypes)
        if indexNamesStr:
            return multi_indexes_small_search(indexNamesStr, matchFields, termFields, returnFields, filterMatch,
                                              filterTerms, useScan)
        else:
            return {}

    def get_document(self, id: str, docType="default") -> dict:
        """
         Returns a document by id

        :param id:          Id of the document
        :param docType:     Type of the document.If not supplied will use default type.
        :return:
        """

        es = get_es_conn()

        try:
            es_wait_ready()
            res = es.get(index=self.typeIndex, doc_type="directory_type", id=docType)
            indexName = res["_source"]["indexName"]
        except exceptions.NotFoundError:
            raise DocumentNotFoundException(docType)

        try:
            doc = es.get(index=indexName, id=id, doc_type=docType)
        except exceptions.NotFoundError:
            raise DocumentNotFoundException(id)
        res = doc["_source"]
        res["id"] = id
        return res

    def delete_document(self, id: str, docType="default"):
        try:
            es = get_es_conn()
            es_wait_ready()
            res = es.get(index=self.typeIndex, doc_type="directory_type", id=docType)
            indexName = res["_source"]["indexName"]
            es.delete(index=indexName, doc_type=docType, id=id)
        except exceptions.NotFoundError:
            raise DocumentNotFoundException(id)

    def add_or_update_schema(self, esPropertiesDelta: dict, docType: str = "default", allowDynamicFields=False):
        """
        Updates the existing mapping with new fields. If the delta is invalid it will throw an error.


        :param docType:
        :param esPropertiesDelta:       Delta in properties schema; Should be {"properties" : {}}
        :return:
        """

        es = get_es_conn()
        indexName = self._create_data_index_if_not_exist(docType, allowDynamicFields)

        es_wait_ready()
        es.indices.put_mapping(index=indexName, body=esPropertiesDelta, doc_type=docType)
        es_wait_ready()

    def _create_data_index_if_not_exist(self, docType="default", allowDynamicFields=True) -> str:
        """
        Will create a data index if it does not exist.
        This method should be considered private.

        :param indexName:
        :param allowDynamicFields:  If false dynamic fields will return an error when the user attempts to add a field not in schema
        :return:    name of the index
        """

        es = get_es_conn()
        try:
            es_wait_ready()
            res = es.get(index=self.typeIndex, doc_type="directory_type", id=docType)
            indexName = res["_source"]["indexName"]
        except exceptions.NotFoundError:
            indexName = self.dataIndexPrefix + "_" + gen_uuid()
            # adding standard name generation for default indexes
            if docType == "default":
                indexName = self.defaultDataIndex

            es.create(index=self.typeIndex, doc_type="directory_type", id=docType, body={"indexName": indexName})

            body = {"settings": {}}
            body["settings"]["index"] = {"max_result_window": MAX_RESULT_WINDOW,
                                         "number_of_shards": get_number_of_shards(),
                                         "number_of_replicas": get_number_of_replicas()}
            body["settings"]["analysis"] = ANALYSIS
            if allowDynamicFields:
                es.indices.create(indexName, body=body)
            else:
                body["mappings"] = {}
                body["mappings"][docType] = {}
                body["mappings"][docType]["dynamic"] = "strict"
                # mapping["mappings"][docType]["properties"] = "{}"
                es.indices.create(indexName, body=body)
            es_wait_ready()
            # artificial pause to make sure index is created and ready
            time.sleep(1)

        return indexName

    def add_document(self, jsonDocument, id: str = None, docType: str = "default", esSchemaMapping: dict = None,
                     createDataIndexIfNotExist: bool = True):
        """
        see add_or_update_document

        """
        return self.add_or_update_document(jsonDocument, id, docType, esSchemaMapping, False, createDataIndexIfNotExist)

    def update_document(self, jsonDocument, id: str = None, docType: str = "default", esSchemaMapping: dict = None):
        """
        see add_or_update_document

        """
        return self.add_or_update_document(jsonDocument, id, docType, esSchemaMapping, True, False)

    def add_or_update_document(self, jsonDocument, id: str = None, docType: str = "default",
                               esProperties: dict = None, update: bool = False, createDataIndexIfNotExist: bool = True):
        """
        Adds a jsonDocument.
        If for a for a give type of document the esSchemaMapping is not compatible, it will throw ConflictingSchemaException.

        Precondition: esSchemaMapping is a valid ElasticSearch mapping, containing a property field.

        If esSchemaMapping is present, it will validate that it is compatible with the schema for docs of doc_type.

        :param id:                  id which will uniquely identify this document between all document of the same type.
        :param jsonDocument:        jsonContaining the document to be indexed.This should be a python object representing the json.. Without schema the whole document is indexed.
        :param docType:            Type of the document. All document with the same type have the same structure.
        :param esProperties:        Properties sections of elastic search mappings. Allows to dynamically change the schema when adding the document.
        :param update:              If true, this function will throw and exception if trying a add an already existing document
        :param createDataIndexIfNotExist:   If true will create a data index if it doest exist for a given type. If false it will throw an error if user attemps to put data into an unexisting index.
        :return:                            Id of the created document
        """

        # TODO, since it will create one index per document type
        # Eventually we will need to merge the different indexes.

        logger = logging.getLogger(__name__)
        es = get_es_conn()

        # get or create index
        indexName = ""
        if createDataIndexIfNotExist:
            indexName = self._create_data_index_if_not_exist(docType, True)
        else:
            try:
                es_wait_ready()
                res = es.get(index=self.typeIndex, doc_type="directory_type", id=docType)
                indexName = res["_source"]["indexName"]
            except exceptions.NotFoundError:
                raise DocumentDirectoryNoSchemaFoundException("No schema found for type:{0}".format(docType))

        # update schema if supplied
        if esProperties:
            esPropertiesFrom = None
            try:
                esMappingFrom = es.indices.get_mapping(index=indexName, doc_type=docType)
                esPropertiesFrom = esMappingFrom[indexName]["mappings"][docType]
            except Exception:
                pass

            if (esPropertiesFrom):
                self.add_or_update_schema(SchemaList.calculate_properties_delta(esPropertiesFrom, esProperties),
                                          docType)
            else:
                self.add_or_update_schema(esProperties, docType)

        if not id:
            id = gen_uuid()

        if update:
            es.update(index=indexName, doc_type=docType, body={"doc": jsonDocument}, id=id)
        else:
            try:
                es.create(index=indexName, doc_type=docType, body=jsonDocument, id=id)
                return id
            except exceptions.ConflictError:
                logger.info("Adding existing document {0} for type {1} in index {2}".format(id, docType, indexName))
                raise DocumentAlreadyExistsException()
            except exceptions.TransportError as te:
                if ("error" in te.info) and ("type" in te.info["error"]) and (
                        te.info["error"]["type"] == "strict_dynamic_mapping_exception"):
                    raise DocumentDoesNotRespectSchemaException("Document id: {0}".format(id))
