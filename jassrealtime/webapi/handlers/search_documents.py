from .base_handler import BaseHandler
from http import HTTPStatus


class SearchDocumentsHandler(BaseHandler):
    def data_received(self, chunk):
        pass

    def options(self):
        self.write_and_set_status(None, HTTPStatus.OK)

    @staticmethod
    def target(corpus_bucket: list) -> tuple:
        if len(corpus_bucket) == 1:
            return corpus_bucket[0], None
        elif len(corpus_bucket) == 2:
            return corpus_bucket[0], corpus_bucket[1]
        else:
            raise ValueError("Invalid corpus/bucket pair: " + str(corpus_bucket))

    def parse_targets(self, targets_argument: str) -> list:
        target_tuples = targets_argument.split(",")
        targets = []

        for target_tuple in target_tuples:
            corpus_bucket = target_tuple.split(":")
            targets.append(self.target(corpus_bucket))

        return targets

    @staticmethod
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
