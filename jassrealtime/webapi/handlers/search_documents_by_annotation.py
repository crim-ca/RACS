import traceback
from http import HTTPStatus

from .search_documents import SearchDocumentsHandler
from ...search.multicorpus.documents_by_annotation import DocumentsByAnnotation
from ...core.settings_utils import get_env_id
from ...security.security_selector import get_autorisation
from ...document.document_corpus import CorpusNotFoundException, DocumentNotFoundException
from .document import MAX_DOCUMENT_SIZE
from .parameter_names import MESSAGE, TRACE


def validate(query):
    if query["operator"] not in ["must", "should", "must_not"]:
        raise ValueError("Invalid operator in query {}.".format(query))
    if query["search_mode"] not in ["noop", "basic", "language", "edge", "path", "ngram"]:
        raise ValueError("Invalid search_mode in query {}.".format(query))


def parse_query(query_argument: str) -> dict:
    """
    A query argument is a tuple of the form:
     boolean_operator:schema_type:attribute:search_mode:search_text

     And eventually, but not implemented yet:
     boolean_operator:schema_type:at_least:count

    :param query_argument:
    :return:
    """
    query_parts = query_argument.split(":")
    if len(query_parts) != 5:
        raise ValueError("Invalid query: " + str(query_argument))
    query = {"operator": query_parts[0],
             "schema_type": query_parts[1],
             "attribute": query_parts[2],
             "search_mode": query_parts[3],
             "text": query_parts[4]}
    validate(query)
    return query


def parse_queries(queries_argument: str) -> list:
    return [parse_query(query_argument) for query_argument in queries_argument.split(",")]


class SearchDocumentsByAnnotationHandler(SearchDocumentsHandler):
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

            # Get corpus id list and their selected bucket ids
            targets_argument = self.get_query_argument("targets", default=None)
            if not targets_argument:
                self.missing_required_field("targets")
                return

            queries_argument = self.get_query_argument("queries", default=None)
            if not queries_argument:
                self.missing_required_field("queries")
                return

            targets = self.parse_targets(targets_argument)
            grouped_targets = self.group_targets(targets)

            queries = parse_queries(queries_argument)

            env_id = get_env_id()
            authorization = get_autorisation(env_id, None, None)
            search = DocumentsByAnnotation(env_id, authorization)
            count, documents = search.documents_by_annotation(grouped_targets, queries, from_index, size)

            self.write_and_set_status({"count": count, "documents": documents}, HTTPStatus.OK)
        except CorpusNotFoundException as exception:
            self.write_and_set_status({MESSAGE: "Corpus not found with id:'{}'".format(exception.corpus_id)},
                                      HTTPStatus.NOT_FOUND)
        except DocumentNotFoundException as exception:
            self.write_and_set_status({MESSAGE: "An annotation references a document not found with document id: '{}'".
                                      format(exception.document_id)},
                                      HTTPStatus.NOT_FOUND)
        except ValueError as error:
            self.write_and_set_status({MESSAGE: str(error)}, HTTPStatus.BAD_REQUEST)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)
