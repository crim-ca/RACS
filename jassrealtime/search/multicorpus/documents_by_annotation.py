from elasticsearch_dsl import Search, Q, A
from elasticsearch_dsl.response import Hit

from jassrealtime.core.document_directory import DocumentNotFoundException
from jassrealtime.search.document import map_search_hit
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

        # Note: using aggregation means we could lose some hits
        # "when there are lots of unique terms, Elasticsearch only returns the top terms"
        # https://www.elastic.co/guide/en/elasticsearch/reference/6.1/search-aggregations-bucket-terms-aggregation.html#CO105-2

        aggregate_by_document_id = A('terms', field='_documentID')
        search.aggs.bucket('doc_ids', aggregate_by_document_id)
        response = search.execute()
        aggregation = response.aggregations['doc_ids']
        count = len(aggregation.buckets)  # we want the count before slicing

        # Note: Elasticsearch DSL slicing API does not work with aggregations, so we slice on aggregation results
        buckets = aggregation.buckets[from_index:from_index+size]

        # Get document information from document id
        # (Document id uniqueness is guaranteed by the aggregation.)
        document_ids = [bucket['key'] for bucket in buckets]
        documents = self.documents_by_ids(grouped_targets, document_ids)

        documents_with_score = self.join_documents_with_score(buckets, documents)

        return count, documents_with_score

    @staticmethod
    def join_documents_with_score(buckets, documents):
        """
        Join documents with score while retaining scoring order.

        We will use the number of matching annotations per document as a naive score.

        The buckets are naturally ordered by descending number of annotations.

        :param buckets:
        :param documents:
        :return:
        """
        document_map = {document['id']: document for document in documents}
        documents_with_score = []
        for bucket in buckets:
            document_id = bucket['key']
            document = document_map.get(document_id, None)
            if document is None:
                raise DocumentNotFoundException(document_id)
            score = bucket['doc_count']
            document['score'] = score
            documents_with_score.append(document)
        return documents_with_score

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

    def documents_by_ids(self, grouped_targets, document_ids):
        indices = self.target_text_document_indices(grouped_targets)
        indices_argument = ','.join(indices)

        es = get_es_conn()
        search = Search(using=es, index=indices_argument)
        search = search.source(["title", "language", "source"])
        search.query = Q({"terms": {"_id": document_ids}})

        documents = [map_search_hit(hit) for hit in search.scan()]

        return documents
