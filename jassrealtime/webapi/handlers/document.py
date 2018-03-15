import json
import traceback
from http import HTTPStatus

from elasticsearch import TransportError

from jassrealtime.webapi.handlers.base_handler import BaseHandler

from jassrealtime.core.master_factory_list import get_master_document_corpus_list
from jassrealtime.document.document_corpus import DocumentAlreadyExistsException, CorpusNotFoundException, \
    DocumentNotFoundException
from jassrealtime.security.security_selector import get_autorisation
from jassrealtime.webapi.handlers.parameter_names import *
from jassrealtime.core.settings_utils import get_env_id

MAX_DOCUMENT_SIZE = 1000


class DocumentFolderHandler(BaseHandler):
    def post(self, corpusId):
        try:
            body = json.loads(self.request.body.decode("utf-8"))

            language = body.get("language")
            if not language:
                self.write_and_set_status({MESSAGE: "Missing required parameters"})
                self.set_status(HTTPStatus.UNPROCESSABLE_ENTITY)
                return

            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)

            docId = body.get("id")  # Note: 'get' defaults to None when key does not exist
            text = body.get("text", "")
            title = body.get("title", "")
            source = body.get("source", "")

            corpus = get_master_document_corpus_list(envId, authorization).get_corpus(corpusId)
            if not language in corpus.languages:
                self.write_and_set_status({MESSAGE: "Document language do not correspond to corpus language"},
                                          HTTPStatus.UNPROCESSABLE_ENTITY)
                return

            docId = corpus.add_text_document(text, title, language, docId, source)

            self.write_and_set_status({"id": docId},
                                      HTTPStatus.OK)
        except DocumentAlreadyExistsException:
            self.write_and_set_status({MESSAGE: "Document with the same id already exists"},
                                      HTTPStatus.CONFLICT)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)

    def options(self, corpusId):
        self.write_and_set_status(None, HTTPStatus.OK)

    def get(self, corpusId):
        """Get documents from corpus according to pagination"""
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

            size = min(size, MAX_DOCUMENT_SIZE)

            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)

            corpus = get_master_document_corpus_list(envId, authorization).get_corpus(corpusId)
            filterTitle = self.get_query_argument("filterTitle", default=None)
            filterSource = self.get_query_argument("filterSource", default=None)
            filterJoin = self.get_query_argument("filterJoin", default=None)
            sortBy = self.get_query_argument("sortBy", default=None)
            sortOrder = self.get_query_argument("sortOrder", default=None)
            documents = corpus.get_text_documents(fromIndex, size, sortBy, sortOrder, filterTitle, filterSource,
                                                  filterJoin)

            self.write_and_set_status({"documents": documents},
                                      HTTPStatus.OK)
        except CorpusNotFoundException:
            self.write_and_set_status({MESSAGE: "Specified corpus not found"},
                                      HTTPStatus.NOT_FOUND)
        except ValueError as ve:
            self.write_and_set_status({MESSAGE: "Invalid 'from' or 'size' parameter"},
                                      HTTPStatus.UNPROCESSABLE_ENTITY)
        except TransportError as te:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "ES TransportError", TRACE: trace},
                                      te.status_code)
        except Exception as e:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)


class DocumentHandler(BaseHandler):
    def get(self, corpusId, documentId):
        """Get a single document from corpus"""
        try:
            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)
            corpus = get_master_document_corpus_list(envId, authorization).get_corpus(corpusId)
            document = corpus.get_text_document(documentId)

            if document is None:
                raise DocumentNotFoundException(documentId)

            self.write_and_set_status(document,
                                      HTTPStatus.OK)
        except CorpusNotFoundException:
            self.write_and_set_status({MESSAGE: "Specified corpus not found"},
                                      HTTPStatus.NOT_FOUND)
        except DocumentNotFoundException:
            self.write_and_set_status({MESSAGE: "Specified document not found"},
                                      HTTPStatus.NOT_FOUND)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)

    def delete(self, corpusId, documentId):
        """Delete a single document an optionally its annotations"""
        try:
            delete_annotations_argument = self.get_query_argument("deleteAnnotations", None)
            if not delete_annotations_argument:
                self.missing_required_field("deleteAnnotations")
                return

            delete_annotations = 'true' == delete_annotations_argument

            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)
            corpus = get_master_document_corpus_list(envId, authorization).get_corpus(corpusId)
            document = corpus.delete_document(documentId, delete_annotations)
            self.write_and_set_status(document,
                                      HTTPStatus.OK)
        except CorpusNotFoundException:
            self.write_and_set_status({MESSAGE: "Specified corpus not found"},
                                      HTTPStatus.NOT_FOUND)
        except DocumentNotFoundException:
            self.write_and_set_status({MESSAGE: "Specified document not found"},
                                      HTTPStatus.NOT_FOUND)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)

    def options(self, corpusId, documentId):
        self.write_and_set_status(None, HTTPStatus.OK)


class DocumentIdsHandler(BaseHandler):
    def options(self, corpusId):
        self.write_and_set_status(None, HTTPStatus.OK)

    def get(self, corpusId):
        try:
            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)
            corpus = get_master_document_corpus_list(envId, authorization).get_corpus(corpusId)

            documentIds = corpus.get_document_ids()

            self.write_and_set_status({"ids": documentIds},
                                      HTTPStatus.OK)
        except CorpusNotFoundException:
            self.write_and_set_status({MESSAGE: "Specified corpus not found"},
                                      HTTPStatus.NOT_FOUND)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)
