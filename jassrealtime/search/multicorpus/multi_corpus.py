import json

from elasticsearch_dsl import Search, Q
from typing import List

from ...core.esutils import get_es_conn
from ...core.schema_list import JSON_SCHEMA_PRIMITIVE_TYPES
from ...core.settings_utils import get_settings
from ...document.bucket import Bucket
from ...document.document_corpus import make_sort_field, make_es_filters, get_master_document_corpus_list, \
    DocumentCorpus
from ...search.document import map_search_hit
from ...security.base_authorization import BaseAuthorization
from ...security.security_selector import get_autorisation


class MultiCorpus:
    def __init__(self, env_id: str, authorization: BaseAuthorization):
        self.env_id = env_id
        self.authorization = authorization

    @staticmethod
    def get_annotations_of_type(self, corpus_ids, schema_type, from_index, size, sort_by, sort_order, filters,
                                filter_join):
        """
        Paginated annotations of the specified type.

        :param self:
        :param corpus_ids:
        :param schema_type:
        :param from_index:
        :param size:
        :param sort_by:
        :param sort_order:
        :param filters:
        :param filter_join:
        :return:
        """
        # Specify all annotations indices for specified corpora
        indices = self.partial_corpora_indices(corpus_ids)

        es = get_es_conn()
        search = Search(using=es, index=indices)
        search = search[
                 from_index:from_index + size]  # We are forced to use the scrolling API to sort (scan won't work)

        # Sort by score if no sort field specified
        if sort_by:
            actual_sort = make_sort_field(sort_by, sort_order)
        else:
            actual_sort = "_score"

        search = search.sort(actual_sort)

        # Need to restrict the search annotations to the given schema type.
        # The alternative is doing a first search to retrieve only the indices of the given schema type.
        # We are not sure which approach is faster overall.
        schema_type_query = Q('term', schemaType=schema_type)

        if filters:
            es_filters = make_es_filters(filters, filter_join)
            # Although we are calling them filters,
            # we cannot actually use an ElasticSearch filter because we would lose score.
            # Scoring is important for sorting searches with analyzers like Ngram, EdgeNgram, etc.
            search = search.query(schema_type_query & es_filters)
        else:
            # Here, we use filter because it's faster and scoring is not relevant
            search = search.filter(schema_type_query)

        count = search.count()

        annotations = [map_search_hit(hit) for hit in search]

        return count, annotations

    def partial_corpora_indices(self, corpus_ids: List[str]) -> str:
        """
            Returns all bucket indices for all the corpus ids specified.
        :param corpus_ids:
        :return:
        """
        # The idiomatic way would be to instantiate a corpus for each corpus Id and then do a search in each corpus
        # for the bucket with the right schema type.
        # As it represent a 2n operation before doing the main search, and fearing latency,
        # I, Jean-François Héon, decided to perform this (possibly premature) optimization.
        settings = get_settings()
        annotation_directory = settings['CLASSES']['DOCUMENT_DIRECTORY']['CLASS_PREFIX']
        data_suffix = settings['CLASSES']['DOCUMENT_DIRECTORY']['INDEX_DATA_SUFFIX']
        index_suffix = '*' + data_suffix + '_*'
        index_prefix = self.env_id + annotation_directory
        indices = []
        for corpus_id in corpus_ids:
            indices.append(index_prefix + corpus_id + index_suffix)
        joined_indices = ','.join(indices)
        return joined_indices

    def query_structure(self, grouped_targets: dict) -> list:
        structure = []

        for corpusId, buckets in grouped_targets.items():
            corpus = self.corpus_from_id(corpusId)
            group_structure = {"corpusId": corpusId, "languages": corpus.languages,
                               "groups": self.buckets_types(corpus, buckets)}
            structure.append(group_structure)

        return structure

    def corpus_from_id(self, corpus_id: str) -> DocumentCorpus:
        authorization = get_autorisation(self.env_id, None, None)
        corpora = get_master_document_corpus_list(self.env_id, authorization)
        return corpora.get_corpus(corpus_id)

    def buckets_types(self, corpus: DocumentCorpus, bucket_ids: list) -> list:
        buckets = []
        for bucket_id in bucket_ids:
            bucket = corpus.get_bucket(bucket_id)
            buckets.append({"bucketId": bucket_id,
                            "name": bucket.name,
                            "types": self.bucket_types(bucket)})
        return buckets

    @staticmethod
    def is_searchable(schema_property: dict) -> bool:
        return schema_property.get("searchable", False)

    def property_query_structure(self, definition: dict) -> dict:
        structure = {"type": definition["type"]}
        self.copy_property_if_exists(definition, structure, "searchModes")
        self.copy_property_if_exists(definition, structure, "language")
        return structure

    @staticmethod
    def copy_property_if_exists(definition, structure, property_name):
        if definition.get(property_name, False):
            structure[property_name] = definition[property_name]

    @staticmethod
    def is_simple_type(definition):
        return definition["type"] in JSON_SCHEMA_PRIMITIVE_TYPES

    def properties(self, schema: dict) -> dict:
        #  Keep simple properties that are searchable. Return name, type (and searchModes for string)
        searchable_properties = {}
        for name, definition in schema["properties"].items():
            if self.is_searchable(definition) and self.is_simple_type(definition):
                searchable_properties[name] = self.property_query_structure(definition)

        return searchable_properties

    def bucket_types(self, bucket: Bucket) -> list:
        schemas_info = bucket.get_schemas_info(includeJson=True)["data"]
        properties_by_schema = []
        for schema_info in schemas_info:
            schema = json.loads(schema_info["jsonSchema"])
            properties_by_schema.append(
                {"schemaType": schema_info["schemaType"], "properties": self.properties(schema)})

        return properties_by_schema
