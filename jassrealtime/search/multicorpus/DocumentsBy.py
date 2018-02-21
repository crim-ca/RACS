from elasticsearch_dsl import Q

from ...core.language_manager import LanguageManager
from ...core.settings_utils import get_language_manager


class DocumentsBy:
    @staticmethod
    def transform(language_manager: LanguageManager, query: dict) -> Q:
        text_field_name = "text"
        if query["search_mode"] == "language":
            text_field_name = text_field_name + "." + language_manager.get_es_analyser(query["language"])

        return Q({"match": {text_field_name: query["text"]}})

    def group_and_transform_queries_by_operator(self, queries: list) -> dict:
        language_manager = get_language_manager()
        operators = ["must", "must_not", "should"]
        grouped_queries = {}
        for operator in operators:
            grouped_queries[operator] = []

        for query in queries:
            for operator in operators:
                if operator == query["operator"]:
                    grouped_queries[operator].append(self.transform(language_manager, query))
                    continue

        return grouped_queries

