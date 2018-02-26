import traceback
from http import HTTPStatus

from ...webapi.handlers.search_documents import SearchDocumentsHandler
from ...core.settings_utils import get_env_id
from ...search.multicorpus.documents_by_text import DocumentsByText
from ...security.security_selector import get_autorisation
from ...document.document_corpus import CorpusNotFoundException
from .document import MAX_DOCUMENT_SIZE
from .parameter_names import MESSAGE, TRACE


def validate(query):
    if query["operator"] not in ["must", "should", "must_not"]:
        raise ValueError("Invalid operator in query {}.".format(query))
    if query["search_mode"] not in ["basic", "language"]:
        raise ValueError("Invalid search_mode in query {}.".format(query))


def parse_query(query_argument: str) -> dict:
    """
    A query argument is a quintuple of the form:
     boolean_operator:search_mode:language:search_text

    Search mode will be 'basic' or 'language', but language field will contain
    the actual target language.

    E.G ...basic:english..., ...language:english...


    :param query_argument:
    :return:
    """
    query_parts = query_argument.split(":")
    if len(query_parts) != 4:
        raise ValueError("Invalid query: " + str(query_argument))
    query = {"operator": query_parts[0],
             "search_mode": query_parts[1],
             "language": query_parts[2],
             "text": query_parts[3]}
    validate(query)
    return query


def parse_queries(queries_argument: str) -> list:
    return [parse_query(query_argument) for query_argument in queries_argument.split(",")]


class SearchDocumentsByTextHandler(SearchDocumentsHandler):
    def get(self):
        try:
            from_index_argument = self.get_query_argument("from", None)
            if not from_index_argument:
                self.missing_required_field("from")
                return

            from_index = int(from_index_argument)
            if from_index < 0:
                self.write_and_set_status({MESSAGE: "'from' must cannot be less than zero"},
                                          HTTPStatus.BAD_REQUEST)
                return

            size_argument = self.get_query_argument("size")
            if not size_argument:
                self.missing_required_field("size")
                return

            size = int(size_argument)

            if size < 1:
                self.write_and_set_status({MESSAGE: "'size' cannot be less than 1"},
                                          HTTPStatus.BAD_REQUEST)
                return

            size = min(size, MAX_DOCUMENT_SIZE)

            # Get corpus id list and their respective bucket ids if any
            targets_argument = self.get_query_argument("targets", default=None)
            if not targets_argument:
                self.missing_required_field("targets")
                return

            targets = self.parse_targets(targets_argument)
            grouped_targets = self.group_targets(targets)

            queries_argument = self.get_query_argument("queries", default=None)
            if not queries_argument:
                self.missing_required_field("queries")
                return

            queries = parse_queries(queries_argument)

            env_id = get_env_id()
            authorization = get_autorisation(env_id, None, None)
            search = DocumentsByText(env_id, authorization)
            count, documents = search.documents_by_text(grouped_targets, queries, from_index, size)

            self.write_and_set_status({"count": count, "documents": documents}, HTTPStatus.OK)
        except CorpusNotFoundException as exception:
            self.write_and_set_status({MESSAGE: "Corpus not found with id:'{}'".format(exception.corpus_id)},
                                      HTTPStatus.NOT_FOUND)
        except ValueError as error:
            self.write_and_set_status({MESSAGE: str(error)}, HTTPStatus.BAD_REQUEST)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)
