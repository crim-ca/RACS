# coding: utf-8

# This module regroup all batch functions used by API on a corpus level.
# The reason search is externalised from the definition of classes itself is that
# search is specific to the application using it. It is thus much simpler to override it later on.

import os, errno, json, sys, urllib.parse
from uuid import uuid1
import logging
import zipfile, glob, shutil, threading
import traceback

from jassrealtime.core.utils import gen_uuid
from ..document.interval import Interval
from ..core.master_factory_list import get_master_bucket_list
from ..security.base_authorization import BaseAuthorization
from typing import List
from ..core.esutils import get_multi_indexes_small_search_query, get_es_conn
from ..core.settings_utils import get_settings, get_file_storage_data_url
from elasticsearch import helpers
from ..search.utils import add_offset_to_query, replaceFieldNames, deleteField
from .tmp_file_storage import TmpFileStorage
from .http_post_file_storage import HttpPostFileStorage


class GeneralSearchInfo:
    pass


class FuncThread(threading.Thread):
    def __init__(self, target, *args):
        self._target = target
        self._args = args
        threading.Thread.__init__(self)

    def run(self):
        self._target(*self._args)


class Corpus:
    def upload_annotations(self, bucketIds: List[str] = [], schemaTypes: List[str] = None,
                           url: str = None, zipFileName: str = None, isSendPut=False,
                           isMultipart: bool = True, multipartFieldName: str = "file"):
        """
        Uploads all document for the current corpus
        :param url: Url to which to upload files
        :param zipFileName: Url to which to upload files
        :return:
        """

        # creates a zip file
        fileStorage = HttpPostFileStorage(url, zipFileName)
        self._create_annotations_zip(fileStorage, bucketIds, schemaTypes)
        fileStorage.flush(True, isSendPut, isMultipart, multipartFieldName)

    def create_tmp_annotations_zip(self, bucketIds: List[str] = [], schemaTypes: List[str] = None,
                                   zipFileName: str = None):
        """
        Creates a zip file containing specified annotations by bucketIds/schemaTypes.
        :param bucketIds:
        :param schemaTypes:
        :param url:
        :param zipFileName:
        :return:    Path to the zip file
        """
        fileStorage = TmpFileStorage(zipFileName)
        self._create_annotations_zip(fileStorage, bucketIds, schemaTypes)
        return fileStorage.zipPath

    def clear_temporary_files(self):
        if self.tmpFileStorage:
            self.tmpFileStorage.clear()

    def _create_annotations_zip(self, fileStorage, bucketIds: List[str] = [], schemaTypes: List[str] = None):
        fileStorage.create_zip_file()
        es = get_es_conn()

        logger = logging.getLogger(__name__)
        bucketList = get_master_bucket_list(self.envId, self.authorization)
        allBuckets = bucketList.get_all_buckets_for_corpus(self.corpusId)
        buckets = []

        # filter useless bucket ids
        if bucketIds:
            for bucket in allBuckets:
                if bucket.id in bucketIds:
                    buckets.append(bucket)
        else:
            buckets = allBuckets

        offsets = None
        for bucket in buckets:
            indices = bucket.dd.get_indices(schemaTypes)

            s = get_multi_indexes_small_search_query(indices=indices)

            esSearchData = s.to_dict()

            if offsets:
                add_offset_to_query(esSearchData, offsets)
            esSearchData["sort"] = [{"_documentID": {"order": "asc"}}]

            annotations = []

            es = get_es_conn()
            page = es.search(body=esSearchData, index=indices, scroll="100m", size=1000)

            sid = page['_scroll_id']
            scroll_size = page['hits']['total']
            currentDocId = None
            annoByType = {}
            while (scroll_size > 0):
                logger.debug("bucket:|" + bucket.id + "| scroll size: " + str(scroll_size))
                for annoRaw in page['hits']['hits']:
                    anno = annoRaw["_source"]
                    anno["annotationId"] = annoRaw["_id"]
                    if not currentDocId:
                        currentDocId = anno["_documentID"]
                    elif currentDocId != anno["_documentID"]:
                        self._save_annotations(annoByType, fileStorage, currentDocId, bucket.id)
                        annoByType = {}
                        currentDocId = anno["_documentID"]

                    schemaType = anno["schemaType"]
                    if not schemaType in annoByType:
                        annoByType[schemaType] = []

                    annoByType[schemaType].append(anno)

                page = es.scroll(scroll_id=sid, scroll='100m')
                sid = page['_scroll_id']
                scroll_size = len(page['hits']['hits'])

            if annoByType:
                self._save_annotations(annoByType, fileStorage, anno["_documentID"], bucket.id)

            # release scroll
            es.clear_scroll(sid)

        fileStorage.close()

    def get_error_url_from_tmpurl(self, tmpUrlId: str):
        """
        Returns an errorUrl associated with tmpUrlId
        :param tmpUrlId:
        :return:
        """
        return urllib.parse.unquote(tmpUrlId)

    def add_annotations(self, bucketId: str, zipFilePath: str):
        """
        :param bucketId:
        :param zipFilePath:     Local location to where to where to save zip file.
        :param errorUrl:        Storage url to where to save errors.
        :return:    {"data" :[error1, ... ,errorN]
        """
        logger = logging.getLogger(__name__)
        fh = open(zipFilePath, 'rb')
        z = zipfile.ZipFile(fh)
        es = get_es_conn()
        bucket = get_master_bucket_list(self.envId, self.authorization).get_bucket(self.corpusId, bucketId)
        indicesPerType = bucket.get_es_index_per_schema_type()
        errorsToReturn = []
        ANNOTATION_ID_FIELD = "annotationId"
        DOCUMENT_ID_FIELD = "_documentID"
        ERROR_DOCUMENT_ID_FIELD = "_documentID"
        ERROR_ANNOTATION_FIELD = "annotation"
        ERROR_MESSAGE_FIELD = "message"
        ERROR_TYPE_FIELD = "errorType"
        ANNOTATION_ERROR_TYPE = "annotation"
        OTHER_ERROR_TYPE = "other"
        MAX_ERRORS_TO_RETURN = 10000
        for name in z.namelist():
            try:
                bulkData = []
                errorAnnotations = []
                data = z.read(name).decode('utf-8')
                jsonData = json.loads(data)
                annotations = jsonData["data"]
                annotationById = {}
                for annotation in annotations:
                    try:
                        # TODO make real annotation validation
                        annotationId = None
                        documentId = None
                        if ANNOTATION_ID_FIELD in annotation:
                            annotationId = annotation[ANNOTATION_ID_FIELD]
                        else:
                            annotationId = gen_uuid()

                        if DOCUMENT_ID_FIELD in annotation:
                            documentId = annotation[DOCUMENT_ID_FIELD]

                        if not "schemaType" in annotation:
                            errorAnnotations.append(
                                {ERROR_ANNOTATION_FIELD: annotation, ERROR_DOCUMENT_ID_FIELD: documentId,
                                 ERROR_TYPE_FIELD: ANNOTATION_ERROR_TYPE,
                                 ERROR_MESSAGE_FIELD: "Missing schemaType"})
                        elif not annotation["schemaType"] in indicesPerType:
                            errorAnnotations.append(
                                {ERROR_ANNOTATION_FIELD: annotation, ERROR_DOCUMENT_ID_FIELD: documentId,
                                 ERROR_TYPE_FIELD: ANNOTATION_ERROR_TYPE,
                                 ERROR_MESSAGE_FIELD: "Bucket doesnt contain a schema for schemaType:{0}".format(
                                     annotation["schemaType"])})
                        elif annotationId in annotationById:
                            errorAnnotations.append(
                                {ERROR_ANNOTATION_FIELD: annotation, ERROR_DOCUMENT_ID_FIELD: documentId,
                                 ERROR_TYPE_FIELD: ANNOTATION_ERROR_TYPE,
                                 ERROR_MESSAGE_FIELD: "Duplicate annotation ID."})
                        else:

                            if ANNOTATION_ID_FIELD in annotation:
                                annotationById[annotationId] = annotation.copy()
                                del annotation[ANNOTATION_ID_FIELD]
                            else:
                                annotationById[annotationId] = annotation

                            recordToAdd = {}
                            recordToAdd["_index"] = indicesPerType[annotation["schemaType"]]
                            recordToAdd["_type"] = annotation["schemaType"]
                            recordToAdd["_id"] = annotationId
                            recordToAdd["_source"] = annotation
                            bulkData.append(recordToAdd)
                    except Exception as e:
                        errorAnnotations.append(
                            {ERROR_ANNOTATION_FIELD: annotation, ERROR_DOCUMENT_ID_FIELD: documentId,
                             ERROR_TYPE_FIELD: ANNOTATION_ERROR_TYPE,
                             ERROR_MESSAGE_FIELD: str(e)})
                        logger.info({ERROR_ANNOTATION_FIELD: annotation, ERROR_DOCUMENT_ID_FIELD: documentId,
                                     ERROR_TYPE_FIELD: ANNOTATION_ERROR_TYPE,
                                     ERROR_MESSAGE_FIELD: str(e)})
                        logger.info({"ERROR TRACEBACK": traceback.format_exc()})
                try:
                    helpers.bulk(es, bulkData)
                except helpers.BulkIndexError as bie:
                    for error in bie.errors:
                        annotationError = {}
                        if "index" in error:
                            if "_id" in error["index"]:
                                if error["index"]["_id"] in annotationById:
                                    annotation = annotationById[error["index"]["_id"]]
                                    documentId = None
                                    if DOCUMENT_ID_FIELD in annotation:
                                        documentId = annotation[DOCUMENT_ID_FIELD]
                                    if "error" in error["index"]:
                                        if "type" in error["index"]["error"] and "reason" in error["index"]["error"]:
                                            if error["index"]["error"]["type"] == "strict_dynamic_mapping_exception":
                                                annotationError = {ERROR_ANNOTATION_FIELD: annotation,
                                                                   ERROR_DOCUMENT_ID_FIELD: documentId,
                                                                   ERROR_TYPE_FIELD: ANNOTATION_ERROR_TYPE,
                                                                   ERROR_MESSAGE_FIELD: "Annotation fields dont respect schema. "
                                                                                        + error["index"]["error"][
                                                                                            "reason"]}
                                            else:
                                                annotationError = {ERROR_ANNOTATION_FIELD: annotation,
                                                                   ERROR_DOCUMENT_ID_FIELD: documentId,
                                                                   ERROR_TYPE_FIELD: ANNOTATION_ERROR_TYPE,
                                                                   ERROR_MESSAGE_FIELD: error["index"]["error"][
                                                                       "reason"]}
                                        else:
                                            annotationError = {ERROR_ANNOTATION_FIELD: annotation,
                                                               ERROR_DOCUMENT_ID_FIELD: documentId,
                                                               ERROR_TYPE_FIELD: ANNOTATION_ERROR_TYPE,
                                                               ERROR_MESSAGE_FIELD: error["index"]["error"]}

                                    else:
                                        annotationError = {ERROR_ANNOTATION_FIELD: annotation,
                                                           ERROR_DOCUMENT_ID_FIELD: documentId,
                                                           ERROR_TYPE_FIELD: ANNOTATION_ERROR_TYPE,
                                                           ERROR_MESSAGE_FIELD: error["index"]}
                        if annotationError:
                            errorAnnotations.append(annotationError)
                            logger.info(annotationError)
                        else:
                            errorAnnotations.append({ERROR_TYPE_FIELD: OTHER_ERROR_TYPE, "message": str(error)})
                            logger.info({ERROR_TYPE_FIELD: OTHER_ERROR_TYPE, "message": str(error)})

                except Exception as e:
                    errorAnnotations.append({ERROR_TYPE_FIELD: OTHER_ERROR_TYPE, "message":
                        "Bulk Insert Failed File: {0}\n{1}".format(name, str(e))})
                    logger.error({ERROR_TYPE_FIELD: OTHER_ERROR_TYPE, "message":
                        "Bulk Insert Failed File: {0}\n{1}".format(name, str(e))})
                    logger.error({"ERROR TRACEBACK": traceback.format_exc()})
            except Exception as e:
                errorAnnotations.append({ERROR_TYPE_FIELD: OTHER_ERROR_TYPE, "message":
                    "Failed to process file: {0}\n{1}".format(name, str(e))})
                logger.error({ERROR_TYPE_FIELD: OTHER_ERROR_TYPE, "message":
                    "Failed to process file: {0}\n{1}".format(name, str(e))})
                logger.error({"ERROR TRACEBACK": traceback.format_exc()})

            for error in errorAnnotations:
                if len(errorsToReturn) < MAX_ERRORS_TO_RETURN:
                    errorsToReturn.append(error)
                else:
                    break

        if errorsToReturn:
            return {"data": errorsToReturn, "totalErrorsCount": len(errorAnnotations)}
        else:
            return None

    def _save_annotations(self, annoByType: List[dict], fileStorage: HttpPostFileStorage, docId: str, bucketId: str):
        for schemaType in annoByType:
            fileStorage.add_json_file(annoByType[schemaType], "{0}.{1}.{2}.json".format(docId, bucketId, schemaType))

    def __init__(self, envId: str, authorization: BaseAuthorization, corpusId: str):
        """

        :param envId:
        :param authorization:
        :param documentId:
        :param corpusId:
        """
        self.envId = envId
        self.authorization = authorization
        self.corpusId = corpusId
        self.tmpFileStorage = None
