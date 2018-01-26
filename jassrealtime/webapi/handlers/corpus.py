import json
import traceback
from http import HTTPStatus

from jassrealtime.core.master_factory_list import get_master_document_corpus_list
from jassrealtime.core.settings_utils import get_env_id
from jassrealtime.document.document_corpus import CorpusAlreadyExistsException, \
    CorpusInvalidFieldException, CorpusNotFoundException

from jassrealtime.security.security_selector import get_autorisation
from jassrealtime.webapi.handlers.parameter_names import MESSAGE, TRACE
from jassrealtime.webapi.handlers.base_handler import BaseHandler
from jassrealtime.webapi.handlers.utils import datetime_to_json_str
from jassrealtime.core.settings_utils import get_language_manager
from jassrealtime.webapi.handlers.utils import valid_es_id

# Parameter names
CORPUS_ID = 'id'
CORPUS_MODIFICATION_DATE = 'modificationDate'
CORPUS_LANGUAGES = 'languages'
CORPUS_DOCUMENT_COUNT = 'documentCount'


class CorporaHandler(BaseHandler):
    def post(self):
        body = self.request.body.decode("utf-8")
        try:
            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)
            json_args = json.loads(body)
            for requiredField in [CORPUS_LANGUAGES]:
                if requiredField not in json_args:
                    self.write_and_set_status({MESSAGE: "Missing required parameters. {0}".format(requiredField)},
                                              HTTPStatus.UNPROCESSABLE_ENTITY)
                    return

            languages = json_args.get(CORPUS_LANGUAGES, None)
            try:
                languageManager = get_language_manager()
                for language in languages:
                    if not languageManager.has_es_analyser(language):
                        self.write_and_set_status({MESSAGE: "Invalid language: " + language},
                                                  HTTPStatus.UNPROCESSABLE_ENTITY)
                        return
            except Exception as e:
                self.write_and_set_status({MESSAGE: "Invalid languages field: " + str(languages)},
                                          HTTPStatus.UNPROCESSABLE_ENTITY)
                return

            corpusId = json_args.get(CORPUS_ID, None)

            if corpusId and not valid_es_id(corpusId):
                self.write_and_set_status({
                                              MESSAGE: "Corpus id invalid '{0}' . CorpusId can only be lowercase,alphanumeric with -_".format(
                                                  corpusId)},
                                          HTTPStatus.UNPROCESSABLE_ENTITY)
                return

            corpora = get_master_document_corpus_list(envId, authorization)
            corpus = corpora.create_corpus(corpusId, languages)
            self.write_and_set_status({"id": corpus.id},
                                      HTTPStatus.OK)
        except CorpusNotFoundException:
            self.write_and_set_status({MESSAGE: "Specified corpus not found"},
                                      HTTPStatus.NOT_FOUND)
        except CorpusInvalidFieldException as ci:
            self.write_and_set_status({MESSAGE: "Invalid field: {0}".format(ci)},
                                      HTTPStatus.UNPROCESSABLE_ENTITY)
        except CorpusAlreadyExistsException:
            self.write_and_set_status({MESSAGE: "Corpus with the same id already exists"},
                                      HTTPStatus.CONFLICT)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)

    def options(self):
        self.write_and_set_status(None, HTTPStatus.OK)

    def get(self):
        try:
            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)
            corpora = get_master_document_corpus_list(envId, authorization)
            corporaInfos = corpora.get_corpuses_list()
            self.write_and_set_status({"data": corporaInfos},
                                      HTTPStatus.OK)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)


class CorpusHandler(BaseHandler):
    def get(self, corpusId):
        try:
            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)
            corpora = get_master_document_corpus_list(envId, authorization)
            corpus = corpora.get_corpus(corpusId)
            info = {
                CORPUS_ID: corpus.id,
                CORPUS_LANGUAGES: corpus.languages,
                CORPUS_MODIFICATION_DATE: datetime_to_json_str(corpus.modificationDate),
                CORPUS_DOCUMENT_COUNT: corpus.get_documents_count()
            }
            self.write_and_set_status(info, HTTPStatus.OK)

        except CorpusNotFoundException:
            self.write_and_set_status({MESSAGE: "Specified corpus not found"},
                                      HTTPStatus.NOT_FOUND)

        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)

    def options(self, corpusId):
        self.write_and_set_status(None, HTTPStatus.OK)

    def put(self, corpusId):
        try:
            body = self.request.body.decode("utf-8")
            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)
            json_args = json.loads(body)

            try:
                languages = json_args.get(CORPUS_LANGUAGES, None)
                if languages:
                    languageManager = get_language_manager()
                    for language in languages:
                        if not languageManager.has_es_analyser(language):
                            self.write_and_set_status({MESSAGE: "Invalid language: " + language},
                                                      HTTPStatus.UNPROCESSABLE_ENTITY)
                            return
            except Exception as e:
                self.write_and_set_status({MESSAGE: "Invalid languages field: " + str(languages)},
                                          HTTPStatus.UNPROCESSABLE_ENTITY)
                return

            corpora = get_master_document_corpus_list(envId, authorization)
            corpus = corpora.update_corpus(corpusId, languages)
            self.write_and_set_status(None, HTTPStatus.NO_CONTENT)

        except CorpusInvalidFieldException as ci:
            self.write_and_set_status({MESSAGE: "Invalid field: {0}".format(ci)},
                                      HTTPStatus.UNPROCESSABLE_ENTITY)

        except CorpusNotFoundException:
            self.write_and_set_status({MESSAGE: "Specified corpus not found"},
                                      HTTPStatus.NOT_FOUND)

        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)

    def delete(self, corpusId):
        try:
            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)
            corpora = get_master_document_corpus_list(envId, authorization)
            corpora.delete_corpus(corpusId)
            self.write_and_set_status(None, HTTPStatus.NO_CONTENT)
        except CorpusNotFoundException:
            self.write_and_set_status({MESSAGE: "Specified corpus not found"},
                                      HTTPStatus.NOT_FOUND)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)
