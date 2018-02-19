from elasticsearch_dsl import Search, Q

from ...search.multicorpus.multi_corpus import MultiCorpus
from ...core.language_manager import LanguageManager
from ...core.settings_utils import get_language_manager
from ...core.esutils import get_es_conn
from ...search.document import map_search_hit
from ...security.base_authorization import BaseAuthorization


class DocumentsByAnnotation:
    def __init__(self, env_id: str, authorization: BaseAuthorization):
        self.env_id = env_id
        self.authorization = authorization
        self.multi_corpus = MultiCorpus(env_id, authorization)

    def documents_by_annotation(self, queries: list, from_index: int, size: int) -> tuple:
        pass
