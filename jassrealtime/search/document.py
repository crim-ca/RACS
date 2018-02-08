# coding: utf-8

# This module regroup all searching functions used by API.
# The reason search is externalised from the definition of classes itself is that
# search is specific to the application using it. It is thus much simpler to override it later on.
from elasticsearch_dsl import Search
from elasticsearch_dsl.response import Hit

from jassrealtime.document.document_corpus import make_sort_field, make_es_filters
from ..document.interval import Interval
from ..core.master_factory_list import get_master_bucket_list
from ..security.base_authorization import BaseAuthorization
from typing import List
from ..core.esutils import get_multi_indexes_small_search_query, get_es_conn
from elasticsearch import helpers
from .utils import add_offset_to_query, replaceFieldNames, deleteField
from ..core.settings_utils import get_scan_scroll_duration, get_nb_documents_per_scan_scroll


class GeneralSearchInfo:
    pass


# Hit used to be called Result http://elasticsearch-dsl.readthedocs.io/en/latest/Changelog.html#id8
def map_search_hit(hit: Hit):
    annotation = hit.__dict__['_d_']
    annotation["id"] = hit.meta.id
    return annotation


class DocumentSearch:
    def search_annotations_for_one_type(self, bucketId: str, schemaType: str, fromIndex: int, size: int,
                                        sortBy: str = None, sortOrder: str = None, filters: str = None,
                                        filterJoin: str = None):
        """
        Search annotation of corpus for one schemaType of one bucket.
        This endpoint exists to facilitate getting a list of documents of a corpus from the metadata document annotation.

        :param bucketId:
        :param schemaType:
        :param fromIndex: ZERO based index
        :param size:
        :param sortBy:
        :param sortOrder:
        :param filters:
        :param filterJoin:
        :return:
        """

        # Inspired from DocumentCorpus.get_text_documents
        es = get_es_conn()

        # Main divergence: indices of one bucket & schema
        bucketList = get_master_bucket_list(self.envId, self.authorization)
        bucket = bucketList.get_bucket(self.corpusId, bucketId)
        indices = bucket.dd.get_indices(docTypes=[schemaType])
        if not indices:
            return 0, []

        search = Search(using=es, index=indices)
        search = search[fromIndex:fromIndex + size]

        # Sort by score if no sort field specified
        if sortBy:
            actualSort = make_sort_field(sortBy, sortOrder)
        else:
            actualSort = "_score"

        search = search.sort(actualSort)

        if filters:
            es_filters = make_es_filters(filters, filterJoin)
            # Although we are calling them filters,
            # we cannot actually use an ElasticSearch filter because we would lose score.
            # Scoring is important for sorting searches with analyzers like Ngram, EdgeNgram, etc.
            search = search.query(es_filters)

        count = search.count()

        annotations = [map_search_hit(hit) for hit in search]

        return count, annotations

    def count_annotations_for_types(self, bucketId: str, schemaTypes: List[str]):
        """
        Return the count of annotations for each schema types.

        Assumption: there is only *one* index per schemaType in each bucket.

        :param bucketId:
        :param schemaTypes:
        :return: map of schema types to annotation count
        """

        es = get_es_conn()

        bucket_list = get_master_bucket_list(self.envId, self.authorization)
        bucket = bucket_list.get_bucket(self.corpusId, bucketId)

        counts = {}
        for schemaType in schemaTypes:
            index = bucket.dd.get_indices(docTypes=[schemaType])
            if index:
                counts[schemaType] = es.count(index)["count"]

        return counts

    def delete_annotations_for_types(self, bucketId: str, schemaTypes: List[str]):
        """
        Delete all bucket annotations for specified schema types

        :param bucketId:
        :param schemaTypes:
        """

        es = get_es_conn()

        bucketList = get_master_bucket_list(self.envId, self.authorization)
        bucket = bucketList.get_bucket(self.corpusId, bucketId)

        # Erase actual indices
        for schemaType in schemaTypes:
            bucket.delete_annotations(schemaType)

    def get_annotations_for_one_type(self, bucketId: str, schemaType: str, offsets: List[Interval] = None):
        """
        Returns all annotations of a given schemaType for a bucket.
        Assuming the following fields maps:
         "_documentID" for document
         "offsets: [{begin: <begin> , end:<end>}]}" for offsets

        :param bucketId:
        :param schemaType:
        :param offset:      Select elements inside a specific offset. TODO: Only works with one offset, which should be
        :return: {
                   documentId : {
                     bucketId: {
                       schemaType : [{annotation1} ... {annotationN}]
                     }
                   }
                 }
        """
        annotations = None
        if offsets:
            raise NotImplementedError()
        else:
            bucketList = get_master_bucket_list(self.envId, self.authorization)
            bucket = bucketList.get_bucket(self.corpusId, bucketId)
            annotations = bucket.dd.small_search(docTypes=[schemaType], filterTerms=self.make_document_filter_terms())
            if annotations:
                replaceFieldNames(annotations, "id", "annotationId")
                deleteField(annotations, "type")

        return {self.corpusId: {bucketId: {schemaType: annotations}}}

    def get_annotations(self, schemaTypesByBucketId: dict, offsets: List[Interval] = None):
        """
        Get annotations for all buckets.
        Assuming the following fields maps:
         "_documentID" for document
         "offsets: [{begin: <begin> , end:<end>}]}" for offsets
        :param schemaTypes: One or more schema types. If empty all schema types will be used.
        :param offsets: "offsets: [{begin: <begin> , end:<end>}]}" for offsets
        :return:
        """

        filterTerms = self.make_document_filter_terms()

        res = {self.corpusId: {}}

        bucketList = get_master_bucket_list(self.envId, self.authorization)
        allBuckets = bucketList.get_all_buckets_for_corpus(self.corpusId)
        buckets = []

        bucketIds = schemaTypesByBucketId.keys()
        # filter useless bucket ids
        if bucketIds:
            for bucket in allBuckets:
                if bucket.id in bucketIds:
                    buckets.append(bucket)
        else:
            buckets = allBuckets
        if offsets:
            for bucket in buckets:
                schemaTypes = schemaTypesByBucketId[bucket.id]
                indices = bucket.dd.get_indices(schemaTypes)
                s = get_multi_indexes_small_search_query(indices=indices, filterTerms=filterTerms)

                esSearchData = s.to_dict()
                add_offset_to_query(esSearchData, offsets)
                res[self.corpusId][bucket.id] = {}
                annotations = []

                es = get_es_conn()
                scanPointer = helpers.scan(es, query=esSearchData, index=indices, scroll=get_scan_scroll_duration(),
                                           size=get_nb_documents_per_scan_scroll())
                for annoRes in scanPointer:
                    # Convert result to dictionary
                    annotation = {}
                    annotation = annoRes["_source"]
                    annotation["annotationId"] = annoRes["_id"]
                    schemaType = annotation["schemaType"]
                    if not schemaType in res[self.corpusId][bucket.id]:
                        res[self.corpusId][bucket.id][schemaType] = []
                    res[self.corpusId][bucket.id][schemaType].append(annotation)

                # remove empty buckets
                if not res[self.corpusId][bucket.id]:
                    res[self.corpusId].pop(bucket.id)

        else:
            for bucket in buckets:
                schemaTypes = schemaTypesByBucketId[bucket.id]
                annotations = bucket.dd.small_search(docTypes=schemaTypes, filterTerms=filterTerms)
                replaceFieldNames(annotations, "id", "annotationId")
                deleteField(annotations, "type")
                res[self.corpusId][bucket.id] = {}
                for annotation in annotations:
                    schemaType = annotation["schemaType"]
                    if not schemaType in res[self.corpusId][bucket.id]:
                        res[self.corpusId][bucket.id][schemaType] = []

                    res[self.corpusId][bucket.id][schemaType].append(annotation)

                # remove empty buckets
                if not res[self.corpusId][bucket.id]:
                    res[self.corpusId].pop(bucket.id)

        return res

    def make_document_filter_terms(self):
        """
        Make appropriate filter term expression according to documentIds

        :return:
        """
        if self.documentIds:
            return {"_documentID": self.documentIds}
        else:
            return {}

    def __init__(self, envId: str, authorization: BaseAuthorization, documentIds: List[str], corpusId: str):
        """

        :param envId:
        :param authorization:
        :param documentIds:
        :param corpusId:
        """
        self.envId = envId
        self.authorization = authorization
        self.documentIds = documentIds
        self.corpusId = corpusId
