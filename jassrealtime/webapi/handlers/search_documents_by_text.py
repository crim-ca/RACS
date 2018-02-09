import traceback
from http import HTTPStatus

from ...document.document_corpus import CorpusNotFoundException
from .base_handler import BaseHandler
from .document import MAX_DOCUMENT_SIZE
from .parameter_names import MESSAGE, TRACE


def parse_query(query_argument: str) -> dict:
    """
    A query argument is a quad of the form :corpus_id:language:text

    We can use the language "basic" to target the standard index.
    :param query_argument:
    :return:
    """
    query_parts = query_argument.split(":")
    if len(query_parts) != 4:
        raise ValueError("Invalid query quad: " + str(query_argument))
    return {"": query_parts[0],
            "corpus_id": query_parts[1],
            "language": query_parts[2],
            "text": query_parts[3]}


def parse_queries(queries_argument: str) -> list:
    return [parse_query(query_argument) for query_argument in queries_argument.split(",")]


class SearchDocumentByTextHandler(BaseHandler):
    def data_received(self, chunk):
        pass

    def options(self):
        self.write_and_set_status(None, HTTPStatus.OK)

    def get(self):
        try:
            from_index_argument = self.get_query_argument("from", None)
            if not from_index_argument:
                self.missing_required_field("from")
                return

            from_index = int(from_index_argument)
            if from_index < 0:
                self.write_and_set_status({MESSAGE: "'from' must cannot be less than zero"},
                                          HTTPStatus.UNPROCESSABLE_ENTITY)
                return

            size_argument = self.get_query_argument("size")
            if not size_argument:
                self.missing_required_field("size")
                return

            size = int(size_argument)

            if size < 1:
                self.write_and_set_status({MESSAGE: "'size' cannot be less than 1"},
                                          HTTPStatus.UNPROCESSABLE_ENTITY)
                return

            size = min(size, MAX_DOCUMENT_SIZE)

            # Get corpus id list and their respective bucket ids if any
            queries_argument = self.get_query_argument("queries", default=None)
            if not queries_argument:
                self.missing_required_field("queries")
                return

            queries = parse_queries(queries_argument)

            documents = something(queries, from_index, size)

            self.write_and_set_status({"documents": documents}, HTTPStatus.OK)
        except CorpusNotFoundException as exception:
            self.write_and_set_status({MESSAGE: "Corpus not found with id:'{}'".format(exception.corpus_id)},
                                      HTTPStatus.NOT_FOUND)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)

    def missing_required_field(self, required_field):
        self.write_and_set_status({MESSAGE: "Missing required parameters. {0}".format(required_field)},
                                  HTTPStatus.UNPROCESSABLE_ENTITY)
