import traceback
from http import HTTPStatus

from jassrealtime.webapi.handlers.base_handler import BaseHandler
from jassrealtime.webapi.handlers.parameter_names import MESSAGE, TRACE


def target(corpus_bucket: list) -> tuple:
    if len(corpus_bucket) == 1:
        return corpus_bucket[0], None
    elif len(corpus_bucket) == 2:
        return corpus_bucket[0], corpus_bucket[1]
    else:
        raise ValueError("Invalid corpus/bucket pair: " + str(corpus_bucket))


def parse_targets(targets_argument: str) -> list:
    target_tuples = targets_argument.split(",")
    targets = []

    for target_tuple in target_tuples:
        corpus_bucket = target_tuple.split(":")
        targets.append(target(corpus_bucket))

    return targets


def group_targets(targets: list) -> dict:
    """
    Group targets by corpus.
    :param targets: pairs of corpus/bucket.
    """
    grouped_targets = {}
    for corpus, bucket in targets:

        if corpus not in grouped_targets:
            grouped_targets[corpus] = []

        if bucket is not None:
            grouped_targets[corpus].append(bucket)

    return grouped_targets

class SearchDocumentQueryStructureHandler(BaseHandler):
    def data_received(self, chunk):
        pass

    def options(self):
        self.write_and_set_status(None, HTTPStatus.OK)

    def get(self):
        try:
            # Get corpus id list and their respective bucket ids if any
            targets_argument = self.get_query_argument("targets", default=None)
            if not targets_argument:
                self.write_and_set_status({MESSAGE: "Missing targets parameter"},
                                          HTTPStatus.UNPROCESSABLE_ENTITY)
                return

            targets = parse_targets(targets_argument)
            grouped_targets = group_targets(targets)

            gnannnnn!!!

            self.write_and_set_status({"structure": grouped_targets}, HTTPStatus.OK)
        except Exception:
            trace = traceback.format_exc().splitlines()
            self.write_and_set_status({MESSAGE: "Internal server error", TRACE: trace},
                                      HTTPStatus.INTERNAL_SERVER_ERROR)
