from elasticsearch_dsl import Search, Q
from elasticsearch_dsl.response import Hit

from .DocumentsBy import DocumentsBy
from ...search.multicorpus.multi_corpus import MultiCorpus
from ...core.esutils import get_es_conn
from ...security.base_authorization import BaseAuthorization


def schema_types_of(queries: list) -> set:
    return {query["schema_type"] for query in queries}


def to_match_query(query: dict) -> tuple:
    attribute_field_name = query["attribute"]
    search_mode = query["search_mode"]
    search_mode_field = "{}.{}".format(attribute_field_name, search_mode)

    type_query = Q({"term": {"schemaType.noop": query["schema_type"]}})
    attribute_query = Q({"match": {search_mode_field: query["text"]}})

    match_query = type_query & attribute_query
    return query["operator"], match_query


class DocumentsByAnnotation(DocumentsBy):
    def __init__(self, env_id: str, authorization: BaseAuthorization):
        self.env_id = env_id
        self.authorization = authorization
        self.multi_corpus = MultiCorpus(env_id, authorization)

    def documents_by_annotation(self, grouped_targets: dict, queries: list, from_index: int, size: int) -> tuple:
        """
        Paginated documents found by annotation.

        :param grouped_targets: buckets grouped by corpus
        :param queries:
        :param from_index:
        :param size:
        :return:
        """

        # Get a set of all schema types
        schema_types = schema_types_of(queries)

        # For pagination/score sorting to work, we need to query all the different corpus indices in the same
        # Elasticsearch query.
        indices = self.targets_indices(grouped_targets, list(schema_types))
        indices_argument = ','.join(indices)

        match_queries = [to_match_query(query) for query in queries]
        grouped_queries = self.group_queries_by_operator(match_queries)

        # Actual annotation search
        es = get_es_conn()
        search = Search(using=es, index=indices_argument)
        search = search.source(["_documentID"])

        search.query = Q('bool',
                         must=grouped_queries["must"],
                         must_not=grouped_queries["must_not"],
                         should=grouped_queries["should"])

        search = search[from_index:from_index + size]

        count = search.count()

        annotations = [self.map_document_id_and_score(hit) for hit in search]

        # TODO aggregate annotation score per document? (doubly so for pagination to work)
        # TODO get document information from document id
        # TODO Can score order be retained by the "join" or should we sort after?

        return count, annotations

    def targets_indices(self, grouped_targets: dict, schema_types: list) -> list:
        return [self.group_indices(corpus_id, buckets_ids, schema_types) for corpus_id, buckets_ids in grouped_targets.items()]

    def group_indices(self, corpus_id, buckets_ids, schema_types: list) -> str:
        corpus = self.multi_corpus.corpus_from_id(corpus_id)
        buckets = [corpus.get_bucket(buckets_id) for buckets_id in buckets_ids]
        bucket_indices = [bucket.dd.get_indices(docTypes=schema_types) for bucket in buckets]
        return ','.join(bucket_indices)

    @staticmethod
    def map_document_id_and_score(hit: Hit) -> dict:
        result = hit.__dict__['_d_']
        result["score"] = hit.meta.score
        return result

