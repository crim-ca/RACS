import logging
import uuid
from io import StringIO

from ..core.master_factory_list import get_master_document_directory_list, get_master_document_corpus_list

from ..core.document_directory import *

SUB_CORPUS_DIRECTORY_ID = "xxxxx_sub_corpus_directory_list_id"


class DocumentSubCorpusException(Exception):
    pass


class SubCorpusAlreadyExistsException(DocumentSubCorpusException):
    pass


class SubCorpusNotFoundException(DocumentSubCorpusException):
    pass


SUB_CORPUS_LIST_PROPERTIES_MAPPING = {
    "properties": {
        "name": {"type": "text"}
    }
}

SUB_CORPUS_MAPPING = {
    "properties": {
        "name": {"type": "text"},
        "corpusId": {"type": "keyword"}
    }
}


class DocumentSubCorpusList():
    """
    Lists all the sub corpuses
    """

    @staticmethod
    def create(envId: str, sett: dict, authorization: BaseAuthorization):
        """
        Contains a list of all sub corpuses. Should be called by document corpus module.

        :return:
        """

        masterList = get_master_document_directory_list(envId, authorization)
        subCorpusList = masterList.create_document_directory(sett['MASTER_DOCUMENT_SUB_CORPUS_ID'])
        time.sleep(1)
        subCorpusList.add_or_update_schema(SUB_CORPUS_LIST_PROPERTIES_MAPPING, "default")

        return DocumentSubCorpusList(envId, sett, authorization)

    def __init__(self, envId, sett, authorization: BaseAuthorization):
        """
        Creates a new corpus list. A corpus list contains all corpuses for a envId.

        :param envId:      envId associated with this corpus.
        :param sett:        settings for the corpus.
        """
        self.envId = envId
        self.classPrefix = sett['CLASS_PREFIX']
        self.masterDocumentSubCorpusId = sett['MASTER_DOCUMENT_SUB_CORPUS_ID']
        self.masterList = get_master_document_directory_list(envId, authorization)
        self.dd = self.masterList.get_directory(self.masterDocumentSubCorpusId)
        self.authorization = authorization

    def delete(self):
        """
        Delete all sub corpuses. Must have authorisation to delete each sub corpus

        :return:
        """
        subCorpusIds = self.dd.small_search(returnFields=[], useScan=False)
        # delete all corpuses
        for metadata in subCorpusIds:
            self.delete_sub_corpus(metadata["id"])

        self.masterList.delete_document_directory(self.dd)

    def create_sub_corpus(self, corpusId: str, name: str):
        """
        This function creates a new document sub corpus. It assumes the function, create_sub_corpus_directory
        was called beforehand. This function needs to be called by document corpus class

        :param authorization:   User authorization object
        :param corpusId:        Id of the corpus
        :param name:            Name for sub corpus class
        :return: Sub Corpus Object
        """
        logger = logging.getLogger(__name__)
        self.authorization.can_create_document_sub_corpus(corpusId)

        id = string_to_id(corpusId + '_' + gen_uuid())
        if self.dd.document_exist(id):
            # Should normally NOT happen. Probability of 2 exact random UUID should be pretty much null.
            message = "Sub Corpus Already Exists with the following id {0} for corpus: {1}".format(id, corpusId)
            logger.error(message)
            raise SubCorpusAlreadyExistsException(message)

        self.dd.add_document(jsonDocument={"name": name, "corpusId": corpusId}, id=id)
        dd = self.masterList.create_document_directory(id)

        # TODO create authorizations
        return DocumentSubCorpus(dd, name, id, corpusId, self.authorization)

    def delete_sub_corpus(self, corpusId: str, subCorpusId: str):
        """
        Delets a sub corpus

        :param authorization:   User authorization object
        :param corpusId:        id of the corpus from which this sub corpus comes from
        :param subCorpusId:     id of sub corpus.
        :return:
        """
        logger = logging.getLogger(__name__)
        self.authorization.can_delete_document_sub_corpus(corpusId, subCorpusId)
        try:
            self.dd.delete_document(subCorpusId)
            self.masterList.delete_document_directory(self.masterList.get_directory(subCorpusId))
        except exceptions.NotFoundError as e:
            logger.info("Sub corpus not: {0}".format(subCorpusId) + str(e))
            raise SubCorpusNotFoundException()
        except DocumentNotFoundException as e:
            logger.info("Sub corpus not: {0}".format(subCorpusId) + str(e))
            raise SubCorpusNotFoundException()
        except DocumentDirectoryDoesntExistsException as e:
            logger.info("Sub corpus not: {0}".format(subCorpusId) + str(e))
            raise SubCorpusNotFoundException()

    def get_sub_corpus(self, corpusId: str, subCorpusId: str):
        """

        :param authorization: User authorization object
        :param id:            Id of sub corpus
        :return:
        """
        self.authorization.can_read_document_sub_corpus(corpusId, subCorpusId)
        try:
            dd = self.masterList.get_directory(subCorpusId)
            subCorpusInfo = self.dd.get_document(subCorpusId)
            return DocumentSubCorpus(dd, subCorpusInfo["name"], subCorpusId, subCorpusInfo["corpusId"],
                                     self.authorization)
        except DocumentNotFoundException:
            raise SubCorpusNotFoundException()

    def get_all_sub_corpuses_for_corpus(self, corpusId: str):
        """
        Returns all sub corpuses for a corpus. The sub corpuses returned depend on user authorization.

        :param authorization:  Authorisation object for sub corpus access
        :param corpusId: The id of the corpus
        :return:
        """
        res = []
        subCorpusInfoArr = self.dd.small_search(termFields={"corpusId": corpusId}, useScan=False)
        for subCorpusInfo in subCorpusInfoArr:
            try:
                self.authorization.can_read_document_sub_corpus(corpusId, subCorpusInfo["id"])
                subCorpusId = subCorpusInfo["id"]
                dd = self.masterList.get_directory(subCorpusId)
                res.append(
                    DocumentSubCorpus(dd, subCorpusInfo["name"], subCorpusId,
                                      subCorpusInfo["corpusId"], self.authorization))
            finally:
                # If no access we simply dont show it.
                pass

        return res


class DocumentSubCorpus():
    """
    Creates a document corpus.

    """

    def __init__(self, dd: DocumentsDirectory, name: str, id: str, corpusId: str, authorization: BaseAuthorization):
        """

        :param authorization:        User authorization object
        :param dd:                  DocumentDirectory associated with this corpus. Contains all documents
        :param name:                Name of corpus
        :param id:                  Unique ID of corpus. (Unique for JIAS)
        :param corpusId:            Id of the corpus containing this sub corpus
        """

        self.authorization = authorization
        self.dd = dd
        self.name = name
        self.id = id
        self.corpusId = corpusId

    def delete_document_ref(self, id: str):
        """
        Deletes a reference to a document

        :param id:
        :param docType:
        :return:
        """
        self.dd.delete_document(id, "default")

    def get_all_document_ref(self):
        """
        :return:
        """
        return self.dd.small_search(returnFields=[])

    def add_document_ref(self, id: str, docType: str):
        """
        Adds a documents reference id to sub corpus. Should only be done by document corpus class.
        Does not validate if the documents exist in sub corpus.

        :param id:                  Id of the document to add. Assumes it exists.
        :param docType:             Type of the document

        :return:
        """

        self.authorization.can_add_document_id_to_sub_corpus(self.corpusId, self.id)
        self.dd.add_document(jsonDocument={"type": docType}, id=id)

    def add_documents_ref(self, docIdsByType: List):
        """
        Adds a documents reference to sub corpus. Should only be done by document corpus class.
        Note this class does not validate the existance of documents

        :param docIdsByType: Dictionnary containing doc references.
                They are grouped the following way { docType : [id1,idn], docType2 ...}

        :return:
        """
        self.authorization.can_add_document_id_to_sub_corpus(self.id)
        buf = StringIO()
        for docType in docIdsByType:
            for id in docIdsByType[docType]:
                # createLine={ "create": { "_index": self., "_type": "{1}", "_id": "{2}" }}
                # buf.write()
                # TODO: Terrible inefficient piece of code. Will change later
                self.dd.add_document(jsonDocument={"type": docType}, id=id)
