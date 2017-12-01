import json, os
import traceback
from http import HTTPStatus
from uuid import uuid1

from jassrealtime.webapi.handlers.base_handler import BaseHandler

from jassrealtime.security.security_selector import get_autorisation
from jassrealtime.webapi.handlers.parameter_names import *
from jassrealtime.core.settings_utils import get_env_id
from jassrealtime.document.interval import Interval
from jassrealtime.batch.corpus import Corpus
from jassrealtime.core.settings_utils import get_jass_tmp_dir
from jassrealtime.document.document_corpus import CorpusNotFoundException
from jassrealtime.batch.http_post_file_storage import UploadUrlFailException

# Elastic search limit values for long
MIN_OFFSET_BEGIN = -9223372036854775808
MAX_OFFSET_END = 9223372036854775807


class BatchAnnotationsUploadHandler(BaseHandler):

    def options(self, corpusId, bucketId):
        self.write_and_set_status(None, HTTPStatus.OK)

    def post(self, corpusId, bucketId):
        try:
            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)

            # download file into temp url location
            tmpUploadFolder = get_jass_tmp_dir()
            fileinfo = self.request.files['file'][0]
            fname = fileinfo['filename']
            ext = os.path.splitext(fname)[1]
            zipName = str(uuid1) + ext
            zipPath = os.path.join(tmpUploadFolder, zipName)
            f = open(zipPath, 'bw')
            f.write(fileinfo['body'])
            f.close()

            # add annotations in batch
            batchCorpus = Corpus(envId, authorization, corpusId)
            errors = batchCorpus.add_annotations(bucketId, zipPath)

            # delete zip file
            os.remove(zipPath)

            if errors:
                self.write_and_set_status(errors,
                                          HTTPStatus.UNPROCESSABLE_ENTITY)
            else:
                self.write_and_set_status(None,
                                          HTTPStatus.OK)

        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)


class BatchAnnotationsDownloadHandler(BaseHandler):
    def options(self, corpusId, bucketId, tmpUrlId):
        self.write_and_set_status(None, HTTPStatus.OK)

    def get(self, corpusId):
        try:
            envId = get_env_id()
            authorization = get_autorisation(envId, None, None)
            schemaTypesStr = self.get_query_argument("schemaTypes",None)
            bucketIdsStr = self.get_query_argument("bucketIds", None)
            schemaTypes = []
            bucketIds = []
            if schemaTypesStr:
                schemaTypes = schemaTypesStr.split(",")
            if bucketIdsStr:
                bucketIds = bucketIdsStr.split(",")

            batchCorpus = Corpus(envId, authorization, corpusId)
            zipPath = batchCorpus.create_tmp_annotations_zip(bucketIds,schemaTypes)
            self.send_zip_file_with_get(zipPath, os.path.basename(zipPath))
            batchCorpus.clear_temporary_files()
        except CorpusNotFoundException:
            self.write_and_set_status({MESSAGE: "Specified corpus not found"},
                                      HTTPStatus.NOT_FOUND)
        except Exception:
            trace = traceback.format_exc().splitlines()
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
            schemaTypesStr = body.get("schemaTypes", None)
            bucketIdsStr = body.get("bucketIds", None)
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

            schemaTypes = []
            bucketIds = []
            if schemaTypesStr:
                schemaTypes = schemaTypesStr.split(",")
            if bucketIdsStr:
                bucketIds = bucketIdsStr.split(",")

            batchCorpus = Corpus(envId, authorization, corpusId)

            batchCorpus.upload_annotations(bucketIds, schemaTypes, destUrl, zipFileName, isSendPut, isMultipart,
                                           multipartFieldName)

            self.write_and_set_status({}, HTTPStatus.OK)
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
