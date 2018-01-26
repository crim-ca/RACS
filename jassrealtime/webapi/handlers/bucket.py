import json
import traceback
from http import HTTPStatus

from jassrealtime.webapi.handlers.base_handler import BaseHandler

from jassrealtime.core.master_factory_list import get_master_document_corpus_list
from jassrealtime.document.bucket import BucketAlreadyExistsException, BucketNotFoundException
from jassrealtime.security.security_selector import get_autorisation
from jassrealtime.webapi.handlers.parameter_names import *
from jassrealtime.core.settings_utils import get_env_id
from jassrealtime.document.document_corpus import CorpusNotFoundException
from jassrealtime.webapi.handlers.utils import valid_es_id


class BucketHandler(BaseHandler):
    def post(self, corpusId):
        try:
            body = json.loads(self.request.body.decode("utf-8"))
            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)
            bucketId = None
            bucketName = None

            if "id" in body:
                bucketId = body["id"]
            if "name" in body:
                bucketName = body["name"]

            if bucketId and not valid_es_id(bucketId):
                self.write_and_set_status({
                                              MESSAGE: "Bucket id invalid '{0}' . BucketId can only be lowercase,alphanumeric with -_".format(
                                                  bucketId)},
                                          HTTPStatus.UNPROCESSABLE_ENTITY)
                return

            bucket = get_master_document_corpus_list(envId, authorization). \
                get_corpus(corpusId).create_bucket(bucketName, bucketId)
            self.write_and_set_status({"id": bucket.id},
                                      HTTPStatus.OK)
        except BucketAlreadyExistsException:
            self.write_and_set_status({MESSAGE: "Bucket with the same id already exists"},
                                      HTTPStatus.CONFLICT)
        except CorpusNotFoundException as err:
            self.write_and_set_status({MESSAGE: "Corpus does not exist.Extra info: '{0}'".format(err)},
                                      HTTPStatus.UNPROCESSABLE_ENTITY)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)

    def options(self, corpusId):
        self.write_and_set_status(None, HTTPStatus.OK)


class BucketFolderHandler(BaseHandler):
    def delete(self, corpusId, bucketId):
        try:
            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)
            corpus = get_master_document_corpus_list(envId, authorization).get_corpus(corpusId)
            corpus.delete_bucket(bucketId)
            self.write_and_set_status(None, HTTPStatus.NO_CONTENT)
        except BucketNotFoundException as err:
            self.write_and_set_status({MESSAGE: "Bucket does not exist.Extra info: '{0}'".format(err)},
                                      HTTPStatus.NOT_FOUND)
        except CorpusNotFoundException as err:
            self.write_and_set_status({MESSAGE: "Corpus does not exist.Extra info: '{0}'".format(err)},
                                      HTTPStatus.NOT_FOUND)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)

    def options(self, corpusId, bucketId):
        self.write_and_set_status(None, HTTPStatus.OK)
