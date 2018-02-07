import json

from elasticsearch_dsl import Search, Q
from typing import List

from jassrealtime.core.esutils import get_es_conn
from jassrealtime.core.schema_list import JSON_SCHEMA_PRIMITIVE_TYPES
from jassrealtime.core.settings_utils import get_settings, get_env_id
from jassrealtime.document.bucket import Bucket
from jassrealtime.document.document_corpus import make_sort_field, make_es_filters, get_master_document_corpus_list, \
    DocumentCorpus
from jassrealtime.search.document import map_search_hit
from jassrealtime.security.security_selector import get_autorisation


def get_metadata_for_documents(corpusIds, schemaType, fromIndex, size, sortBy, sortOrder, filters,
                               filterJoin):
    # Specify all annotations indices for specified corpora
    indices = partial_corpora_indices(corpusIds)

    es = get_es_conn()
    search = Search(using=es, index=indices)
    search = search[fromIndex:fromIndex + size]  # We are forced to use the scrolling API to sort (scan won't work)

    # Sort by score if no sort field specified
    if sortBy:
        actual_sort = make_sort_field(sortBy, sortOrder)
    else:
        actual_sort = "_score"

    search = search.sort(actual_sort)

    # Need to restrict the search annotations to the given schema type.
    # The alternative is doing a first search to retrieve only the indices of the given schema type.
    # We are not sure which approach is faster overall.
    schema_type_query = Q('term', schemaType=schemaType)

    if filters:
        es_filters = make_es_filters(filters, filterJoin)
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


def partial_corpora_indices(corpus_ids: List[str]) -> str:
    # The idiomatic way would be to instantiate a corpus for each corpus Id and then do a search in each corpus
    # for the bucket with the right schema type.
    # As it represent a 2n operation before doing the main search, and fearing latency,
    # I, Jean-François Héon, decided to perform this (possibly premature) optimization.
    settings = get_settings()
    annotation_directory = settings['CLASSES']['DOCUMENT_DIRECTORY']['CLASS_PREFIX']
    data_suffix = settings['CLASSES']['DOCUMENT_DIRECTORY']['INDEX_DATA_SUFFIX']
    index_suffix = '*' + data_suffix + '_*'
    index_prefix = get_env_id() + annotation_directory
    indices = []
    for corpus_id in corpus_ids:
        indices.append(index_prefix + corpus_id + index_suffix)
    joined_indices = ','.join(indices)
    return joined_indices


def query_structure(grouped_targets: dict) -> list:
    structure = []

    for corpusId, buckets in grouped_targets.items():
        corpus = corpus_from_id(corpusId)
        group_structure = {"corpusId": corpusId, "languages": corpus.languages,
                           "groups": buckets_types(corpus, buckets)}
        structure.append(group_structure)

    return structure


def corpus_from_id(corpus_id: str) -> DocumentCorpus:
    env_id = get_env_id()
    authorization = get_autorisation(env_id, None, None)
    corpora = get_master_document_corpus_list(env_id, authorization)
    return corpora.get_corpus(corpus_id)


def buckets_types(corpus: DocumentCorpus, bucket_ids: list) -> list:
    buckets = []
    for bucket_id in bucket_ids:
        buckets.append({"bucketId": bucket_id, "types": bucket_types(corpus.get_bucket(bucket_id))})
    return buckets


def is_searchable(schema_property: dict) -> bool:
    return schema_property.get("searchable", False)


def property_query_structure(definition: dict) -> dict:
    structure = {"type": definition["type"]}
    if definition.get("searchModes", False):
        structure["searchModes"] = definition["searchModes"]
    return structure


def is_simple_type(definition):
    return definition["type"] in JSON_SCHEMA_PRIMITIVE_TYPES


def properties(schema: dict) -> List:
    #  Keep simple properties that are searchable. Return name, type (and searchModes for string)
    searchable_properties = {}
    for name, definition in schema["properties"].items():
        if is_searchable(definition) and is_simple_type(definition):
            searchable_properties[name] = property_query_structure(definition)

    return searchable_properties


def bucket_types(bucket: Bucket) -> dict:
    schemas_info = bucket.get_schemas_info(includeJson=True)["data"]
    properties_by_schema = []
    for schema_info in schemas_info:
        schema = json.loads(schema_info["jsonSchema"])
        properties_by_schema.append({"type": schema_info["schemaType"], "Properties": properties(schema)})

    return properties_by_schema
