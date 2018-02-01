from unittest import TestCase

from jassrealtime.webapi.handlers.search_documents_query_structure import parse_targets, group_targets


class TestTargets(TestCase):
    def test_parse_targets(self):
        targets_string = "corpus_bucket,c4,c1:b1,c1:b2,c3,c4:b1,c1"
        expected = [('corpus_bucket', None), ('c4', None), ('c1', 'b1'), ('c1', 'b2'), ('c3', None), ('c4', 'b1'),
                    ('c1', None)]
        actual = parse_targets(targets_string)
        self.assertListEqual(actual, expected)

    def test_group_targets(self):
        targets = [('corpus_bucket', None), ('c4', None), ('c1', 'b1'), ('c1', 'b2'), ('c3', None), ('c4', 'b1'),
                   ('c1', None)]

        expected = {
            "corpus_bucket": [],
            "c4": [
                "b1"
            ],
            "c1": [
                "b1",
                "b2"
            ],
            "c3": []
        }

        actual = group_targets(targets)
        self.assertDictEqual(actual, expected)
