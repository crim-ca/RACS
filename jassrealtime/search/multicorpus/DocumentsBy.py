from elasticsearch_dsl.response import Hit


class DocumentsBy:
    @staticmethod
    def map_hit_with_score(hit: Hit) -> dict:
        document = hit.__dict__['_d_']
        document["id"] = hit.meta.id
        document["score"] = hit.meta.score
        return document

    @staticmethod
    def group_queries_by_operator(queries: list) -> dict:
        operators = ["must", "must_not", "should"]
        grouped_queries = {}
        for operator in operators:
            grouped_queries[operator] = []

        for query_operator, query in queries:
            for operator in operators:
                if operator == query_operator:
                    grouped_queries[operator].append(query)
                    continue

        return grouped_queries
