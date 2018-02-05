# coding: utf-8

# pagination is a hard problem
# http://blog.salsify.com/engineering/efficient-pagination-in-sql-and-elasticsearch
# https://abhishek376.wordpress.com/2014/04/05/elasticsearch-sorting-and-paging-nested-documents/
from elasticsearch_dsl.response import Hit

from .bucket import *
from .document_sub_corpus import *
from ..core.master_factory_list import get_master_document_directory_list, \
    get_master_document_sub_corpus_list, get_master_bucket_list

from ..core.esutils import ES_DATE_FORMAT, convert_datetime_to_es, convert_es_date_to_datetime
from ..core.settings_utils import get_language_manager, get_scan_scroll_duration, \
    get_nb_documents_per_scan_scroll

from functools import reduce
from operator import or_, and_


class DocumentCorpusException(Exception):
    pass


class CorpusAlreadyExistsException(DocumentCorpusException):
    pass


class CorpusNotFoundException(DocumentCorpusException):
    def __init__(self, corpus_id):
        self.corpus_id = corpus_id


class CorpusInvalidFieldException(DocumentCorpusException):
    pass


class CorpusDoesntContainLanguageException(DocumentCorpusException):
    pass


MODIFICATION_DATE_FIELD = "modificationDate"
LANGUAGES_FIELD = "languages"
CORPUS_DOCUMENT_COUNT = 'documentCount'

# list of default properties
CORPUS_LIST_PROPERTIES_MAPPING = {
    "properties": {
        LANGUAGES_FIELD: {"type": "keyword"},
        # when the corpus was modified. UTC
        MODIFICATION_DATE_FIELD: {
            "type": "date",
            "format": ES_DATE_FORMAT
        }
    }
}

LANGUAGE_MAPPINGS = {
    "french": {
        "properties": {
            "language": {
                "type": "keyword"
            },
            "title": {
                "type": "keyword"
            },
            "source": {
                "type": "keyword"
            },
            "text": {
                "type": "text",
                "analyzer": "french"
            },
        }
    },
    "english": {
        "properties": {
            "language": {
                "type": "keyword",
            },
            "title": {
                "type": "keyword",
            },
            "source": {
                "type": "keyword",
            },
            "text": {
                "type": "text",
                "analyzer": "english"
            }
        }
    }
}


class DocumentCorpusList:
    @staticmethod
    def create(envId: str, sett: dict, authorization: BaseAuthorization):
        """
        Creates a new corpus list. A corpus list contains all corpuses for a envId.

        :param envId:      envId associated with this corpus.
        :param sett:        settings for the corpus.
        :return:            DocumentCorpusList
        """

        masterList = get_master_document_directory_list(envId, authorization)
        corpusList = masterList.create_document_directory(sett['MASTER_DOCUMENT_CORPUS_ID'])
        time.sleep(1)
        corpusList.add_or_update_schema(CORPUS_LIST_PROPERTIES_MAPPING, "default", True)
        DocumentSubCorpusList.create(envId, sett['DOCUMENT_SUB_CORPUS'], authorization)

        return DocumentCorpusList(envId, sett, authorization)

    def __init__(self, envId, sett, authorization: BaseAuthorization):
        """
        Creates a new corpus list. A corpus list contains all corpuses for a envId.

        :param envId:      envId associated with this corpus.
        :param sett:        settings for the corpus.
        """
        self.envId = envId
        self.classPrefix = sett['CLASS_PREFIX']
        self.masterDocumentCorpusId = sett['MASTER_DOCUMENT_CORPUS_ID']
        self.masterList = get_master_document_directory_list(envId, authorization)
        self.dd = self.masterList.get_directory(self.masterDocumentCorpusId)
        self.authorization = authorization

    def delete(self):
        """
        Deletes all corpuses and sub corpuses for this envId. Must have authorisation to delete each corpus
        :return:
        """

        corpusIds = self.dd.small_search(returnFields=[], useScan=False)
        # delete all corpuses
        for metadata in corpusIds:
            self.delete_corpus(metadata["id"])

        # delete all subcorpuses
        subCorpusList = get_master_document_sub_corpus_list(self.envId, self.authorization)
        subCorpusList.delete()

        # delete the list
        masterList = get_master_document_directory_list(self.envId, self.authorization)
        masterList.delete_document_directory(self.dd)

    def create_corpus(self, id: str = None, languages: dict = ["english"]):
        """
        This function creates a new document corpus. It assumes the function, create_corpus_directory
        was called beforehand.

        :param languages:
        :param id:       Unique ID identifying the corpus, if none supplied one will be generated.
                         Should be alphanumeric _- lowercase.
        :return: Corpus Object
        """
        logger = logging.getLogger(__name__)
        self.authorization.can_create_document_corpus()

        if id:
            if self.dd.document_exist(id):
                logger.info("Corpus Already Exists with the following id:".format(id))
                raise CorpusAlreadyExistsException(id)
        else:
            id = gen_uuid()

        # validate data
        if not languages:
            raise CorpusInvalidFieldException("Missing languages for corpus")

        languageManager = get_language_manager()
        for language in languages:
            if not languageManager.has_es_analyser(language):
                raise CorpusInvalidFieldException("Invalid language {0}".format(language))

        corpus = {LANGUAGES_FIELD: languages, MODIFICATION_DATE_FIELD: self.generate_modification_date()}

        utcDateTime = convert_es_date_to_datetime(corpus[MODIFICATION_DATE_FIELD])

        self.dd.add_document(corpus, id)

        # creating listing for sub corpus
        dd = self.masterList.create_document_directory(id, None, False)
        # TODO create authorizations
        docCorpus = DocumentCorpus(self.envId, dd, id, self.authorization, [], utcDateTime)
        for language in languages:
            docCorpus.add_language(language)

        return docCorpus

    def update_corpus(self, id: str, languages: [str] = None):
        """
        See create corpus for field descriptions.

        :param id:
        :param languages:
        :return:
        """

        docCorpus = self.get_corpus(id)
        languageManager = get_language_manager()
        for language in languages:
            if not languageManager.has_es_analyser(language):
                raise CorpusInvalidFieldException("Invalid language {0}".format(language))

        for language in docCorpus.languages:
            if language not in languages:
                raise CorpusInvalidFieldException("Can not remove language from corpus {0}".format(language))

        corpus = {LANGUAGES_FIELD: languages}

        corpus[MODIFICATION_DATE_FIELD] = self.generate_modification_date()

        self.dd.update_document(corpus, id)

        for language in languages:
            docCorpus.add_language(language)

    def generate_modification_date(self):
        return convert_datetime_to_es(datetime.utcnow())

    def get_corpuses_list(self):
        """
        Lists all corpuses. This only return corpus metadata.

        :return: { data : [ {id:"id",name:NAME_FIELD,platformId:PLATFORM_ID_FIELD}]}
        """

        # TODO get security to filter list

        res = self.dd.small_search(useScan=False)
        corpuses = []
        for doc in res:
            corpusInfo = doc

            # getting documents is very slow. will need to be changed eventually
            corp = self.get_corpus(doc["id"])
            del corpusInfo["type"]
            doc[CORPUS_DOCUMENT_COUNT] = corp.get_documents_count()
            corpuses.append(corpusInfo)

        return corpuses

    def get_corpus(self, id: str):
        """
        Gets the corpus.

        :param id: Id of the corpus
        :return:
        """

        self.authorization.can_get_document_corpus(id)

        try:
            corpusInfo = self.dd.get_document(id)
            dd = self.masterList.get_directory(id)
            languages = corpusInfo.get(LANGUAGES_FIELD)
            modificationDate = convert_es_date_to_datetime(corpusInfo.get(MODIFICATION_DATE_FIELD))
            # todo add metadata
            return DocumentCorpus(self.envId, dd, id, self.authorization, languages, modificationDate)
        except DocumentNotFoundException:
            raise CorpusNotFoundException(id)

    def delete_corpus(self, id: str):
        """
        Deletes a corpus

        :param id:                  Id of the corpus to delete
        :return:
        """
        logger = logging.getLogger(__name__)
        self.authorization.can_delete_document_corpus(id)
        corpus = self.get_corpus(id)
        subCorpusList = corpus.get_all_sub_corpuses()

        # removes all buckets
        buckets = corpus.get_buckets()
        for bucket in buckets:
            corpus.delete_bucket(bucket.id)

        # removes all sub corpuses
        for subCorpus in subCorpusList:
            try:
                corpus.delete_sub_corpus(subCorpus.id)
            except Exception as e:
                logger.warning("Something went wrong when delete subCorpus for document {0}. {1}".format(id, str(e)))
                pass

        # deletes all indexes associated with this corpus.
        self.dd.delete_document(id)
        self.masterList.delete_document_directory(corpus.dd)


def add_filter(filters: List[tuple], fieldName: str, fieldValue: str):
    if fieldValue is not None:
        filters.append((fieldName, fieldValue))


def make_sort_field(sortBy: str, sortOrder: str):
    #  indexed id field is _uid!
    if sortBy == "id":
        field = "_uid"
    else:
        field = sortBy

    if sortOrder == "desc":
        return "-" + field
    else:
        return field


def make_es_filters(filters: List[tuple], filterJoin: str):
    esFilters = [Q({"match": {fieldName: fieldValue}}) for fieldName, fieldValue in filters]

    if filterJoin == "or":
        joinOperator = or_
    else:
        joinOperator = and_

    return reduce(joinOperator, esFilters)


class DocumentCorpus():
    """
        Creates a document corpus.
    """

    @staticmethod
    def addPropertyIfExists(document, hit, propertyName):
        values = getattr(hit, propertyName, None)
        if values and len(values) > 0:
            document[propertyName] = values

    @staticmethod
    def mapDocument(hit: dict):
        document = {
            "id": hit.get("id"),
            "title": hit.get("title"),
            "text": hit.get("text"),
            "source": hit.get("source"),
            "language": hit.get("language")
        }
        return document

    def __init__(self, envId: str, dd: DocumentsDirectory, id: str, authorization: BaseAuthorization,
                 languages: [str] = None, modificationDate: datetime = None):
        """
            corpus["language"] = doc.get("language")

        :param authorization:        User authorization object
        :param envId:              DocumentDirectory associated with this corpus. Contains all documents
        :param dd:                  DocumentDirectory containing all the document of the corpus
        :param id:                  Unique ID of corpus. (Unique for JIAS per envId)
        """

        self.languages = languages
        self.authorization = authorization
        self.envId = envId
        self.dd = dd
        self.id = id
        self.subCorpusType = id  # We create an index which will contain all the sub corpuses.
        self.subCorpusList = get_master_document_sub_corpus_list(self.envId, self.authorization)
        self.bucketList = get_master_bucket_list(self.envId, self.authorization)
        self.modificationDate = modificationDate

    def add_language(self, language: str):
        """
        Adds a new language to the corpus if it doesn't exist
            raises LanguageNotSupported if not specified analyser associated with the language.

            Note: we use the resulting es analyser name instead of french, fr_ca, fr_f, etc to have a consistent
            index name :param language: :return:
        """
        languageManager = get_language_manager()
        if language not in self.languages:
            analyser = languageManager.get_es_analyser(language)
            self.dd.add_or_update_schema(LANGUAGE_MAPPINGS[analyser], analyser, False)
            self.languages.append(language)

    def add_text_document(self, text: str, title: str, language: str, id=None, source="") -> str:
        """
        Adds a text to document. Containing Unicode? (TODO) text. This does not guarantee uniqueness of id
        if lots of documents are added at the same time.

        :param language:
        :param title:
        :param text:             Text to put in the document
        :param id:               Id of the document. Will be autogenerated if not present. Will throw an exception if already exists.
                                 Document id is unique for the corpus.
        :return:                 Document Id
        """

        self.authorization.can_read_document_from_corpus(self.id)

        if not id:
            id = gen_uuid()

        # If there is duplicate ids the system will tell.
        if language not in self.languages:
            raise CorpusDoesntContainLanguageException(str(language))
        if self.dd.document_exist(id, language):
            raise DocumentAlreadyExistsException

        document = dict(text=text, title=title, language=language, source=source)

        self.dd.add_document(document, id, language)

        return id

    def mapDocumentHit(self, hit: Hit):
        document = {"id": hit.meta.id}
        self.addPropertyIfExists(document, hit, 'title')
        self.addPropertyIfExists(document, hit, 'language')
        self.addPropertyIfExists(document, hit, 'source')

        return document

    def get_documents_count(self):
        indices = self.dd.get_indices(self.languages).split(",")
        es = get_es_conn()
        totalDocumentCount = 0
        for index in indices:
            escount = es.count(index)
            totalDocumentCount = totalDocumentCount + escount["count"]
        return totalDocumentCount

    def get_text_document(self, documentId):
        rawDoc = self.dd.small_search(docTypes=self.languages, filterTerms={"_id": documentId}, useScan=False)
        if not rawDoc:
            raise DocumentNotFoundException(documentId)

        document = self.mapDocument(rawDoc[0])

        return document

    def get_text_documents(self, fromIndex: int, size: int, sortBy: str = None, sortOrder: str = None,
                           filterTitle: str = None, filterSource: str = None, filterJoin: str = None):
        es = get_es_conn()
        search = Search(using=es, index=self.dd.get_indices(self.languages))
        search = search[fromIndex:fromIndex + size]
        search = search.source(["title", "language", "source"])

        if sortBy:
            search = search.sort(make_sort_field(sortBy, sortOrder))

        filters = []
        add_filter(filters, "title", filterTitle)
        add_filter(filters, "source", filterSource)
        if filters:
            es_filters = make_es_filters(filters, filterJoin)
            search = search.filter(es_filters)

        search.execute()

        documents = [self.mapDocumentHit(hit) for hit in search]

        return documents

    def get_document_ids(self) -> List[str]:
        """
        Get document ids of all the documents of the corpus
        """
        es = get_es_conn()
        search = Search(using=es, index=self.dd.get_indices(self.languages))
        search = search.source(["_id"])
        search = search.params(scroll=get_scan_scroll_duration(), size=get_nb_documents_per_scan_scroll())
        #  Only 10 hits if don't use scan.
        documentIds = [hit.meta.id for hit in search.scan()]

        return documentIds

    def delete_document(self, documentId):
        doc = self.get_text_document(documentId)
        if not doc:
            raise DocumentNotFoundException()
        self.dd.delete_document(documentId, doc["language"])

    def get_all_documents_ids_and_types(self):
        """
        Return the ids and types of all the documents.

        :return:
        """

        self.authorization.can_read_document_from_corpus(self.id)

        return self.dd.small_search(docTypes=[], returnFields=[])

    ### SUB CORPUS

    def get_all_sub_corpuses(self):
        """
        Returns a list containing all sub corpuses names and ids, accessible by this user.

        :return:
        """
        return self.subCorpusList.get_all_sub_corpuses_for_corpus(self.id)

    def create_sub_corpus(self, name: str) -> DocumentSubCorpus:
        """
        Creates a new empty sub corpus for this directory

        :param name:    Name of the corpus
        :return:        Sub corpus object
        """

        return self.subCorpusList.create_sub_corpus(self.id, name)

    def get_sub_corpus(self, subCorpusId: str) -> DocumentSubCorpus:
        return self.subCorpusList.get_sub_corpus(self.id, subCorpusId)

    def delete_sub_corpus(self, subCorpusId):
        """
        Removes a sub corpus with subCorpusId.
        Throws an exception if subCorpusId does not exist

        :param subCorpusId:
        :return:
        """
        self.subCorpusList.delete_sub_corpus(self.id, subCorpusId)
        time.sleep(1)  # cheap wait to make sure ES has time to delete sub corpus.

    def add_document_to_sub_corpus(self, id, subCorpus: DocumentSubCorpus, docType="default"):
        """
        Adds a document to sub corpus. Will check if the document exists. Otherwise throw and error

        :param id:          Id of the document
        :param subCorpus:   Sub corpus to which to add the document
        :param docType:     Type of the document. By default uses default type
        :return:
        """

        self.authorization.can_read_document_from_corpus(self.id)
        subCorpus.add_document_ref(id, docType)

    def add_documents_to_sub_corpus(self, docIdsByType: dict, subCorpus: DocumentSubCorpus):
        """

        :param docIdsByType: Dictionary containing doc references.
                They are grouped the following way { docType : [id1,idn], docType2 ...}

        :param subCorpus:      Sub corpus to which to add documents
        :return:
        """

        self.authorization.can_read_document_from_corpus(self.id)
        subCorpus.add_documents_ref(docIdsByType)
        # TODO validate

    # Buckets

    def create_bucket(self, name: str, bucketId: str = None) -> Bucket:
        """
        Creates a bucket which targets documents

        :param name:                    Name of the bucket
        :param bucketId:                      Id of the bucket (If None it will generate one automatically). Will throw an exception if id already exists.
        :return:                        Created Bucket
        """
        return self.bucketList.create_bucket(name, self.id, bucketId)

    def get_buckets(self) -> List[Bucket]:
        """
        :return: List of buckets associated with this corpus
        """
        return self.bucketList.get_all_buckets_for_corpus(self.id)

    def get_bucket(self, bucketId: str) -> Bucket:
        """
        Returns a bucket of a given ID. If not found will throw an exception.

        :param bucketId:  Id of the bucket. Note that there is no validation if the bucket is part fo this corpus.
        :return:    Bucket
        """
        return self.bucketList.get_bucket(self.id, bucketId)

    def delete_bucket(self, bucketId: str):
        """
        Deletes a bucket with a given id.

        :param bucketId:
        :return:
        """
        self.bucketList.delete_bucket(self.id, bucketId)
