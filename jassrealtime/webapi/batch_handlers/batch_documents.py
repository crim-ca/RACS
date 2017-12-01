import json,os
import traceback
from http import HTTPStatus

from jassrealtime.webapi.handlers.base_handler import BaseHandler

from jassrealtime.security.security_selector import get_autorisation
from jassrealtime.webapi.handlers.parameter_names import *
from jassrealtime.core.settings_utils import get_env_id
from jassrealtime.batch.document_corpus import DocumentCorpus
from jassrealtime.document.document_corpus import CorpusNotFoundException
from jassrealtime.batch.http_post_file_storage import UploadUrlFailException
import logging


class BatchDocumentsHandler(BaseHandler):
    def options(self, corpusId):
        self.write_and_set_status(None, HTTPStatus.OK)

    def get(self, corpusId):
        try:
            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)
            documentCorpus = DocumentCorpus(envId, authorization, corpusId)
            zipPath = documentCorpus.get_documents_zip()
            zipName = os.path.basename(zipPath)
            self.send_zip_file_with_get(zipPath,zipName)
            documentCorpus.clear_temporary_files()

        except CorpusNotFoundException:
            self.write_and_set_status({MESSAGE: "Specified corpus not found"},
                                      HTTPStatus.NOT_FOUND)
        except Exception:
            trace = traceback.format_exc().splitlines()
            logger = logging.getLogger(__name__)
            logger.error(str(trace))
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)

    def post(self, corpusId):
        try:

            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)

            body = json.loads(self.request.body.decode("utf-8"))

            zipFileName = body.get("zipFileName")
            destUrl = body.get("destUrl")
            isSendPut = body.get("isSendPut", True)
            isMultipart = body.get("isMultipart", False)
            multipartFieldName = body.get("multipartFieldName", "")
            if not zipFileName:
                self.write_and_set_status({MESSAGE: "Missing 'zipFileName' parameter"},
                                          HTTPStatus.UNPROCESSABLE_ENTITY)
                return
            if not destUrl:
                self.write_and_set_status({MESSAGE: "Missing 'destUrl' parameter"},
                                          HTTPStatus.UNPROCESSABLE_ENTITY)
                return

            documentCorpus = DocumentCorpus(envId, authorization, corpusId)
            documentCorpus.upload_documents(destUrl, zipFileName,isSendPut,isMultipart,multipartFieldName)

            self.write_and_set_status({},HTTPStatus.OK)
        except CorpusNotFoundException:
            self.write_and_set_status({MESSAGE: "Specified corpus not found"},
                                      HTTPStatus.NOT_FOUND)
        except UploadUrlFailException as upErr:
            self.write_and_set_status({MESSAGE: "Upload failed due: {0}".format(str(upErr))},
                                      HTTPStatus.UNPROCESSABLE_ENTITY)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)