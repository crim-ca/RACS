from elasticsearch_dsl import Search, Q

from ...search.multicorpus.multi_corpus import MultiCorpus
from ...core.language_manager import LanguageManager
from ...core.settings_utils import get_language_manager
from ...core.esutils import get_es_conn
from ...search.document import map_search_hit
from ...security.base_authorization import BaseAuthorization


def schema_types_of(queries: list) -> set:
    return {query["schema_type"] for query in queries}


class DocumentsByAnnotation:
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
        # indices_argument = ','.join(indices)
        # TODO actual annotation search

        # shall we refactor to extract annotation search returning all annotations fields or just doc ids
        # TODO get meta_document information from docid (or search corpus documents?)

        return len(indices), indices

    def targets_indices(self, grouped_targets: dict, schema_types: list) -> list:
        return [self.group_indices(corpus_id, buckets_ids, schema_types) for corpus_id, buckets_ids in grouped_targets.items()]

    def group_indices(self, corpus_id, buckets_ids, schema_types: list) -> str:
        corpus = self.multi_corpus.corpus_from_id(corpus_id)
        buckets = [corpus.get_bucket(buckets_id) for buckets_id in buckets_ids]
        bucket_indices = [bucket.dd.get_indices(docTypes=schema_types) for bucket in buckets]
        return ','.join(bucket_indices)

