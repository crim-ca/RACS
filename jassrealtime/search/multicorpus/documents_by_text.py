from jassrealtime.core.settings_utils import get_language_manager
from jassrealtime.search.multicorpus.multi_corpus import corpus_from_id


def query_index(query: dict) -> str:
    """
    Get query index from the corpus id and the chosen language.

    :param query:
    :return:
    """
    corpus = corpus_from_id(query["corpus_id"])
    indices_per_doc_type = corpus.dd.get_indices_per_doc_type()
    language_analyser = get_language_manager().get_es_analyser(query["language"])
    return indices_per_doc_type[language_analyser]


def queries_indices(queries: list) -> list:
    return [query_index(query) for query in queries]


def documents_by_text(queries: list, from_index:int, size:int) -> list:
    """
    Paginated documents found by text.

    :param queries:
    :param from_index:
    :param size:
    :return:
    """
    # For pagination/score sorting to work, we need to query all the different corpus indices in the same
    # Elasticsearch query.
    indices = queries_indices(queries)
    indices_argument = ','.join(indices)

    # hmmm, now we have to sticks the `must`, `should` and `must not`
    # queries in 3 different bags 😭

    # How to restrict each query to the right language index?
    # Maybe it will work automatically via the query text.<language>



    return indices

