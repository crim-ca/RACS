from ..security.base_authorization import BaseAuthorization
from ..core.esutils import get_es_conn
from ..core.settings_utils import get_scan_scroll_duration,get_nb_documents_per_scan_scroll
from elasticsearch import helpers
from .http_post_file_storage import HttpPostFileStorage
from .tmp_file_storage import TmpFileStorage
from elasticsearch_dsl import Search, Q
from jassrealtime.core.master_factory_list import get_master_document_corpus_list
import logging
import time

NB_OF_DOCUMENTS_TO_ADD_BEFORE_LOGGING = 1000

class DocumentCorpus:
    def __init__(self, envId: str, authorization: BaseAuthorization, corpusId: str):
        """
        :param envId:
        :param authorization:
        :param corpusId:
        """
        self.envId = envId
        self.authorization = authorization
        self.corpusId = corpusId
        self.tmpFileStorage = None

    def get_documents_zip(self,zipFileName: str = None):
        """
        Creates a zip of all documents of the corpus and returns the path to them.

        :param zipFileName: Name of the created zip file. If not supplied it will be automatically
                generated. If exists, the existing file will be replaced.
        :return: path to the document in thee
        """
        logger = logging.getLogger(__name__)
        self.tmpFileStorage = TmpFileStorage(zipFileName)
        self.tmpFileStorage.create_zip_file()
        es = get_es_conn()
        corpus = get_master_document_corpus_list(self.envId, self.authorization).get_corpus(self.corpusId)
        search = Search(using=es, index=corpus.dd.get_indices(corpus.languages))
        search = search.fields(["text"])
        search = search.params(scroll=get_scan_scroll_duration(),size=get_nb_documents_per_scan_scroll())

        start = time.time()
        count = 0
        logger.info("Adding documents to zip: {0}".format(self.corpusId))
        for result in search.scan():
            self.tmpFileStorage.add_utf8_file(result.text[0], str(result.meta.id) + ".txt")
            count +=1
            if count % NB_OF_DOCUMENTS_TO_ADD_BEFORE_LOGGING == 0:
                end = time.time()
                logger.info("Time to add documents {0} to {1} : {2} seconds"
                            .format(count - NB_OF_DOCUMENTS_TO_ADD_BEFORE_LOGGING,count,end-start))
                start = end

        end = time.time()
        logger.info("Time to add documents {0} to {1} : {2} seconds"
                    .format(count - count % NB_OF_DOCUMENTS_TO_ADD_BEFORE_LOGGING,count,end-start))
        self.tmpFileStorage.close()

        return self.tmpFileStorage.zipPath

    def clear_temporary_files(self):
        if self.tmpFileStorage:
            self.tmpFileStorage.clear()

    def upload_documents(self, url: str = None, zipFileName: str = None,isSendPut = False
                         ,isMultipart: bool = True,multipartFieldName: str = "file"):
        """
        Uploads all document for the current corpus
        :param url: Url to which to upload files
        :param zipFileName: Url to which to upload files
        :return:
        """

        # creates a zip file
        logger = logging.getLogger(__name__)
        fileStorage = HttpPostFileStorage(url, zipFileName)
        fileStorage.create_zip_file()
        es = get_es_conn()
        corpus = get_master_document_corpus_list(self.envId, self.authorization).get_corpus(self.corpusId)
        search = Search(using=es, index=corpus.dd.get_indices(corpus.languages))
        search = search.fields(["text"])
        search = search.params(scroll=get_scan_scroll_duration(),size=get_nb_documents_per_scan_scroll())

        start = time.time()
        count = 0
        logger.info("Adding documents to zip: {0}".format(self.corpusId))
        for result in search.scan():
            fileStorage.add_utf8_file(result.text[0], str(result.meta.id) + ".txt")
            count += 1
            if count % NB_OF_DOCUMENTS_TO_ADD_BEFORE_LOGGING == 0:
                end = time.time()
                logger.info("Time to add documents {0} to {1} : {2} seconds"
                            .format(count - NB_OF_DOCUMENTS_TO_ADD_BEFORE_LOGGING, count, end - start))
                start = end

        end = time.time()
        logger.info("Time to add documents {0} to {1} : {2} seconds"
                    .format(count - count % NB_OF_DOCUMENTS_TO_ADD_BEFORE_LOGGING, count, end - start))

        fileStorage.flush(True,isSendPut, isMultipart, multipartFieldName)
