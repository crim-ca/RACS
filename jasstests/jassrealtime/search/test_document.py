import unittest

from jassrealtime.document.bucket import *
from jassrealtime.core.master_factory_list import get_schema_list, get_env_list, \
    get_master_document_corpus_list
from jassrealtime.core.env import EnvAlreadyExistWithSameIdException
from jassrealtime.search.document import *
from jasstests.jassrealtime.core.test_schema_list import JSON_SCHEMA_WITH_SCHEMA_TYPE_BASIC

envIdReadOnly = "unitsearch_"
authorizationReadOnly = None


class MyTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            global envIdReadOnly
            global authorizationReadOnly

            setting = get_settings()
            authorizationReadOnly = BaseAuthorization("unitsearch_", None, None, None)
            envList1 = get_env_list(authorizationReadOnly)
            try:
                envList1.create_env(envIdReadOnly)
                MyTestCase.populateData()
            except EnvAlreadyExistWithSameIdException:
                # pass
                # Read only so we dont need to change it if it exists.
                # If search environement exists we do dothing
                envList1.delete_env(envIdReadOnly)
                envList1.create_env(envIdReadOnly)
                MyTestCase.populateData()
        finally:
            pass

    def recreate_read_write_env(self):
        try:
            setting = get_settings()
            self.envId = "unittest_"
            self.authorization = BaseAuthorization(self.envId, None, None, None)
            envList1 = get_env_list(self.authorization)
            try:
                envList1.create_env(self.envId)
            except EnvAlreadyExistWithSameIdException:
                time.sleep(1)
                envList1.delete_env(self.envId)
                envList1.create_env(self.envId)
        finally:
            pass

    @classmethod
    def populateData(cls):
        global envIdReadOnly
        global authorizationReadOnly
        # Copy paste from test corpus
        corpus = get_master_document_corpus_list(envIdReadOnly, authorizationReadOnly).create_corpus("corpus1")
        bucket1 = corpus.create_bucket("bucket1", "bucket1")
        bucket2 = corpus.create_bucket("bucket2", "bucket2")

        sentencesS = {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "targetType": "document",
            "schemaType": "sentence",
            "type": "object",
            "required": [
                "schemaType",
                "_corpusID",
                "_documentID",
                "sentence"
            ],
            "properties": {
                "schemaType": {
                    "type": "string",
                    "description": "Schema type",
                    "searchable": True,
                    "searchModes": ["noop"],
                    "locked": True
                },
                "_documentID": {
                    "type": "string",
                    "description": "Internal document GUID",
                    "searchable": True,
                    "searchModes": ["noop"],
                    "locked": True
                },
                "_corpusID": {
                    "type": "string",
                    "description": "Internal Corpus GUID",
                    "searchable": True,
                    "searchModes": ["noop"],
                    "locked": True
                },
                "sentence": {
                    "type": "string",
                    "description": "Sentence in a document",
                    "searchable": True,
                    "searchModes": ["basic"],
                    "locked": True
                }
            }
        }

        tokenS = {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "targetType": "document_surface1d",
            "schemaType": "token",
            "type": "object",
            "required": [
                "schemaType",
                "_corpusID",
                "_documentID",
                "sentence"
            ],
            "properties": {
                "schemaType": {
                    "type": "string",
                    "description": "Schema type",
                    "searchable": True,
                    "searchModes": ["noop"],
                    "locked": True
                },
                "_documentID": {
                    "type": "string",
                    "description": "Internal document GUID",
                    "searchable": True,
                    "searchModes": ["noop"],
                    "locked": True
                },
                "_corpusID": {
                    "type": "string",
                    "description": "Internal Corpus GUID",
                    "searchable": True,
                    "searchModes": ["noop"],
                    "locked": True
                },
                "word": {
                    "type": "string",
                    "description": "Word in a document",
                    "searchable": True,
                    "searchModes": ["basic"],
                    "locked": True
                },
                "length": {
                    "type": "integer",
                    "description": "Length of a word",
                    "searchable": True,
                    "searchModes": ["noop"],
                    "locked": True
                },
                "category": {
                    "type": "string",
                    "description": "category of the word",
                    "searchable": True,
                    "searchModes": ["basic"],
                    "locked": True
                },
                "offsets": {
                    "searchable": True,
                    "locked": True,
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "properties": {
                            "begin": {"type": "integer", "minimum": 0},
                            "end": {"type": "integer", "minimum": 0}
                        }
                    }
                }
            }
        }

        tokenwithlemmaS = {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "targetType": "document_surface1d",
            "schemaType": "tokenwithlemma",
            "type": "object",
            "required": [
                "schemaType",
                "_corpusID",
                "_documentID",
                "sentence"
            ],
            "properties": {
                "schemaType": {
                    "type": "string",
                    "description": "Schema type",
                    "searchable": True,
                    "searchModes": ["basic"],
                    "locked": True
                },
                "_documentID": {
                    "type": "string",
                    "description": "Internal document GUID",
                    "searchable": True,
                    "searchModes": ["noop"],
                    "locked": True
                },
                "_corpusID": {
                    "type": "string",
                    "description": "Internal Corpus GUID",
                    "searchable": True,
                    "searchModes": ["noop"],
                    "locked": True
                },
                "word": {
                    "type": "string",
                    "description": "Word in a document",
                    "searchable": True,
                    "searchModes": ["basic"],
                    "locked": True
                },
                "length": {
                    "type": "integer",
                    "description": "Length of a word",
                    "searchable": True,
                    "searchModes": ["noop"],
                    "locked": True
                },
                "lemma": {
                    "type": "string",
                    "description": "Lemma of a word",
                    "searchable": True,
                    "searchModes": ["basic"],
                    "locked": True
                },
                "category": {
                    "type": "string",
                    "description": "category of the word",
                    "searchable": True,
                    "searchModes": ["basic"],
                    "locked": True
                },
                "offsets": {
                    "searchable": True,
                    "locked": True,
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "properties": {
                            "begin": {"type": "integer", "minimum": 0},
                            "end": {"type": "integer", "minimum": 0}
                        }
                    }
                }
            }
        }
        schemaList = get_schema_list(envIdReadOnly, authorizationReadOnly)
        sentenceSID = schemaList.add_json_schema(jsonSchema=sentencesS)
        tokenSID = schemaList.add_json_schema(jsonSchema=tokenS, nestedFields="offsets")
        tokenwithlemmaSID = schemaList.add_json_schema(jsonSchema=tokenwithlemmaS, nestedFields="offsets")

        bucket1.add_or_update_schema_to_bucket(sentenceSID, sentencesS["schemaType"],
                                               TargetType(sentencesS["targetType"]), {})
        bucket1.add_or_update_schema_to_bucket(tokenSID, tokenS["schemaType"], TargetType(tokenS["targetType"]), {})
        bucket1.add_or_update_schema_to_bucket(tokenwithlemmaSID, tokenwithlemmaS["schemaType"],
                                               TargetType(tokenwithlemmaS["targetType"]), {})
        bucket2.add_or_update_schema_to_bucket(tokenSID, tokenS["schemaType"], TargetType(tokenS["targetType"]), {})
        bucket2.add_or_update_schema_to_bucket(tokenwithlemmaSID, tokenwithlemmaS["schemaType"],
                                               TargetType(tokenwithlemmaS["targetType"]), {})
        time.sleep(1)
        #  sentences
        # bucket 1
        bucket1.add_annotation({"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "sentence",
                                "sentence": "Les algorithmes de colonies de fourmis sont des algorithmes inspirés du comportement des fourmis."},
                               "sentence")

        bucket1.add_annotation({"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "sentence",
                                "sentence": "Le café liégeois doit son appellation à la résistance de l’armée belge lors de la bataille des forts de Liège d’août 1914."},
                               "sentence")

        # bucket1
        # token
        bucket1.add_annotation({"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "token", "word": "Les",
                                "offsets": [{"begin": 0, "end": 3}], "length": 3, "category": "DET:ART"}, "token")
        bucket1.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "token", "word": "algorithmes",
             "offsets": [{"begin": 4, "end": 15}], "length": 11, "category": "NOM"}, "token")
        bucket1.add_annotation({"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "token", "word": "de",
                                "offsets": [{"begin": 28, "end": 30}, {"begin": 16, "end": 18}], "length": 2,
                                "category": "PRP"}, "token")
        bucket1.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "token", "word": "colonies",
             "offsets": [{"begin": 19, "end": 27}], "length": 8, "category": "NOM"}, "token")
        bucket1.add_annotation({"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "token", "word": "fourmis",
                                "offsets": [{"begin": 31, "end": 38}], "length": 7, "category": "NOM"}, "token")
        bucket1.add_annotation({"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "token", "word": "sont",
                                "offsets": [{"begin": 39, "end": 43}], "length": 4, "category": "VER:pres"}, "token")
        # some doc 2 annotations
        bucket1.add_annotation({"_documentID": "doc2", "_corpusID": "corpus1", "schemaType": "token", "word": "des",
                                "offsets": [{"begin": 44, "end": 47}], "length": 3, "category": "PRP:det"}, "token")
        bucket1.add_annotation(
            {"_documentID": "doc2", "_corpusID": "corpus1", "schemaType": "token", "word": "algorithmes",
             "offsets": [{"begin": 48, "end": 59}], "length": 11, "category": "NOM"}, "token")

        # tokenwithlemma
        bucket1.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "Le",
             "offsets": [{"begin": 98, "end": 100}], "length": 2, "lemma": "le", "category": "DET:ART"},
            "tokenwithlemma")
        bucket1.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "café",
             "offsets": [{"begin": 101, "end": 105}], "length": 4, "lemma": "café", "category": "NOM"},
            "tokenwithlemma")
        bucket1.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "liégeois",
             "offsets": [{"begin": 106, "end": 114}], "length": 8, "lemma": "liégeois", "category": "ADJ"},
            "tokenwithlemma")
        bucket1.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "doit",
             "offsets": [{"begin": 115, "end": 119}], "length": 4, "lemma": "devoir", "category": "VER:pres"},
            "tokenwithlemma")
        bucket1.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "son",
             "offsets": [{"begin": 120, "end": 123}], "length": 3, "lemma": "son", "category": "DET:POS"},
            "tokenwithlemma")
        bucket1.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "appellation",
             "offsets": [{"begin": 124, "end": 135}], "length": 11, "lemma": "appellation", "category": "NOM"},
            "tokenwithlemma")
        bucket1.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "à",
             "offsets": [{"begin": 136, "end": 137}], "length": 1, "lemma": "à", "category": "PRP"}, "tokenwithlemma")
        bucket1.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "la",
             "offsets": [{"begin": 138, "end": 140}], "length": 2, "lemma": "le", "category": "DET:ART"},
            "tokenwithlemma")
        bucket1.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "résistance",
             "offsets": [{"begin": 141, "end": 151}], "length": 10, "lemma": "résistance", "category": "NOM"},
            "tokenwithlemma")
        bucket1.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "de",
             "offsets": [{"begin": 152, "end": 154}], "length": 2, "lemma": "de", "category": "PRP"}, "tokenwithlemma")
        bucket1.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "l",
             "offsets": [{"begin": 155, "end": 156}], "length": 1, "lemma": None, "category": "NOM"}, "tokenwithlemma")
        bucket1.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "armée",
             "offsets": [{"begin": 157, "end": 162}], "length": 5, "lemma": "armer", "category": "VER:pper"},
            "tokenwithlemma")
        bucket1.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "belge",
             "offsets": [{"begin": 163, "end": 168}], "length": 5, "lemma": "belge", "category": "ADJ"},
            "tokenwithlemma")
        bucket1.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "lors",
             "offsets": [{"begin": 169, "end": 173}], "length": 4, "lemma": "lors", "category": "ADV"},
            "tokenwithlemma")
        bucket1.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "de",
             "offsets": [{"begin": 174, "end": 176}], "length": 2, "lemma": "de", "category": "PRP"}, "tokenwithlemma")
        bucket1.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "la",
             "offsets": [{"begin": 177, "end": 179}], "length": 2, "lemma": "le", "category": "DET:ART"},
            "tokenwithlemma")
        bucket1.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "bataille",
             "offsets": [{"begin": 180, "end": 188}], "length": 8, "lemma": "bataille", "category": "NOM"},
            "tokenwithlemma")

        # bucket2
        # token
        bucket2.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "token", "word": "algorithmes",
             "offsets": [{"begin": 48, "end": 59}], "length": 11, "category": "NOM"}, "token")
        bucket2.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "token", "word": "inspirés",
             "offsets": [{"begin": 60, "end": 68}], "length": 8, "category": "VER:pper"}, "token")
        bucket2.add_annotation({"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "token", "word": "du",
                                "offsets": [{"begin": 69, "end": 71}], "length": 2, "category": "PRP:det"}, "token")
        bucket2.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "token", "word": "comportement",
             "offsets": [{"begin": 72, "end": 84}], "length": 12, "category": "NOM"}, "token")
        bucket2.add_annotation({"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "token", "word": "des",
                                "offsets": [{"begin": 85, "end": 88}], "length": 3, "category": "PRP:det"}, "token")
        bucket2.add_annotation({"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "token", "word": "fourmis",
                                "offsets": [{"begin": 89, "end": 96}], "length": 7, "category": "NOM"}, "token")
        bucket2.add_annotation({"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "token", "word": ".",
                                "offsets": [{"begin": 96, "end": 97}], "length": 1, "category": "SENT"}, "token")

        # tokenwithlemma
        bucket2.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "armée",
             "offsets": [{"begin": 157, "end": 162}], "length": 5, "lemma": "armer", "category": "VER:pper"},
            "tokenwithlemma")
        bucket2.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "belge",
             "offsets": [{"begin": 163, "end": 168}], "length": 5, "lemma": "belge", "category": "ADJ"},
            "tokenwithlemma")
        bucket2.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "lors",
             "offsets": [{"begin": 169, "end": 173}], "length": 4, "lemma": "lors", "category": "ADV"},
            "tokenwithlemma")
        bucket2.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "de",
             "offsets": [{"begin": 174, "end": 176}], "length": 2, "lemma": "de", "category": "PRP"}, "tokenwithlemma")
        bucket2.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "la",
             "offsets": [{"begin": 177, "end": 179}], "length": 2, "lemma": "le", "category": "DET:ART"},
            "tokenwithlemma")
        bucket2.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "bataille",
             "offsets": [{"begin": 180, "end": 188}], "length": 8, "lemma": "bataille", "category": "NOM"},
            "tokenwithlemma")
        bucket2.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "des",
             "offsets": [{"begin": 189, "end": 192}], "length": 3, "lemma": "du", "category": "PRP:det"},
            "tokenwithlemma")
        bucket2.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "forts",
             "offsets": [{"begin": 193, "end": 198}], "length": 5, "lemma": "fort", "category": "NOM"},
            "tokenwithlemma")
        bucket2.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "de",
             "offsets": [{"begin": 199, "end": 201}], "length": 2, "lemma": "de", "category": "PRP"}, "tokenwithlemma")
        bucket2.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "Liège",
             "offsets": [{"begin": 202, "end": 207}], "length": 5, "lemma": "Liège", "category": "NAM"},
            "tokenwithlemma")
        bucket2.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "d",
             "offsets": [{"begin": 208, "end": 209}], "length": 1, "lemma": None, "category": "VER:futu"},
            "tokenwithlemma")
        bucket2.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "août",
             "offsets": [{"begin": 210, "end": 214}], "length": 4, "lemma": "août", "category": "NOM"},
            "tokenwithlemma")
        bucket2.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": "1914",
             "offsets": [{"begin": 215, "end": 219}], "length": 4, "lemma": "@card@", "category": "NUM"},
            "tokenwithlemma")
        bucket2.add_annotation(
            {"_documentID": "doc1", "_corpusID": "corpus1", "schemaType": "tokenwithlemma", "word": ".",
             "offsets": [{"begin": 219, "end": 220}], "length": 1, "lemma": ".", "category": "SENT"}, "tokenwithlemma")
        time.sleep(1)

    def test_get_annotations_by_document_one_type(self):
        global envIdReadOnly
        global authorizationReadOnly
        ds = DocumentSearch(envIdReadOnly, authorizationReadOnly, "doc1", "corpus1")
        sentences = ds.get_annotations_for_one_type("bucket1", "sentence")
        self.assertEqual(len(sentences["corpus1"]["bucket1"]["sentence"]), 2)
        tokens = ds.get_annotations_for_one_type("bucket1", "token")
        self.assertEqual(len(tokens["corpus1"]["bucket1"]["token"]), 6)

    def test_count_annotations_for_types(self):
        global envIdReadOnly
        global authorizationReadOnly
        ds = DocumentSearch(envIdReadOnly, authorizationReadOnly, "doc1", "corpus1")
        counts = ds.count_annotations_for_types("bucket1", ["sentence"])
        self.assertEqual(counts["sentence"], 2)

    def test_count_annotations_for_type_basic(self):
        """
            Test annotation count for schemaType indexed as basic instead of noop.
            Note: Not sure it should be permitted at all to allow schemaType with main index different than noop.
        """
        global envIdReadOnly
        global authorizationReadOnly
        schema = json.loads(JSON_SCHEMA_WITH_SCHEMA_TYPE_BASIC)

        corpus = get_master_document_corpus_list(envIdReadOnly, authorizationReadOnly).create_corpus()
        bucket = corpus.create_bucket("bucket")
        schema_id = get_schema_list(envIdReadOnly, authorizationReadOnly).add_json_schema_as_hash(schema, False,
                                                                                                  nestedFields=[
                                                                                                      "offsets"])
        time.sleep(1)

        schema_type = "CHUNK_ap"
        bucket.add_or_update_schema_to_bucket(schema_id, schema_type, TargetType("document_surface1d"), {})
        time.sleep(1)

        annotations = [
            {
                "_documentID": "98ff06a6-02dd-11e8-b82a-0242ac12001f",
                "_corpusID": "rqgbf20180126",
                "length": 14,
                "string": "contemporaines",
                "schemaType": "CHUNK_ap",
                "offsets": [
                    {
                        "end": 449,
                        "begin": 435
                    }
                ],
            },
            {
                "_documentID": "98ff06a6-02dd-11e8-b82a-0242ac12001f",
                "_corpusID": "rqgbf20180126",
                "length": 13,
                "string": "plus anciens,",
                "schemaType": "CHUNK_ap",
                "offsets": [
                    {
                        "end": 593,
                        "begin": 580
                    }
                ],
            },
            {
                "_documentID": "98ff06a6-02dd-11e8-b82a-0242ac12001f",
                "_corpusID": "rqgbf20180126",
                "length": 9,
                "string": "coloniale",
                "schemaType": "CHUNK_ap",
                "offsets": [
                    {
                        "end": 693,
                        "begin": 684
                    }
                ],
            }
        ]

        for annotation in annotations:
            bucket.add_annotation(annotation, schema_type)

        time.sleep(1)

        ds = DocumentSearch(envIdReadOnly, authorizationReadOnly, "doc1", corpus.id)
        counts = ds.count_annotations_for_types(bucket.id, [schema_type])
        self.assertEqual(counts[schema_type], len(annotations))

    def test_get_annotations(self):
        global envIdReadOnly
        global authorizationReadOnly
        ds = DocumentSearch(envIdReadOnly, authorizationReadOnly, "doc1", "corpus1")
        annotations = ds.get_annotations({"bucket1": ["token", "tokenwithlemma"]})
        self.assertEqual(len(annotations["corpus1"]["bucket1"]["token"]), 6)
        self.assertEqual(len(annotations["corpus1"]["bucket1"]["tokenwithlemma"]), 17)

    def test_get_annotations_with_offset(self):
        global envIdReadOnly
        global authorizationReadOnly
        ds = DocumentSearch(envIdReadOnly, authorizationReadOnly, "doc1", "corpus1")
        # supposed to be 2 docs, one with 2 offsets, and one with ofset start at 19
        offsets = [Interval(16, 28, False, False, False)]
        annotations = ds.get_annotations(
            {"bucket1": ["token", "tokenwithlemma"], "bucket2": ["token", "tokenwithlemma"]}, offsets)
        self.assertEqual(len(annotations["corpus1"]["bucket1"]["token"]), 2)
        self.assertTrue("bucket2" not in annotations["corpus1"])

        offsets2 = [Interval(43, 170, False, False, False)]
        # From bucket 1: token => 1 , tokenwithlemma => 14
        # From bucket 2: token => 7 , tokenwithlemma => 3
        annotations2 = ds.get_annotations(
            {"bucket1": ["token", "tokenwithlemma"], "bucket2": ["token", "tokenwithlemma"]}, offsets2)
        self.assertEqual(len(annotations2["corpus1"]["bucket1"]["token"]), 1)
        self.assertEqual(len(annotations2["corpus1"]["bucket1"]["tokenwithlemma"]), 14)
        self.assertEqual(len(annotations2["corpus1"]["bucket2"]["token"]), 7)
        self.assertEqual(len(annotations2["corpus1"]["bucket2"]["tokenwithlemma"]), 3)

        # same as offset2 by only with bucket1
        annotations3 = ds.get_annotations({"bucket1": ["token", "tokenwithlemma"]}, offsets2)
        self.assertEqual(len(annotations2["corpus1"]["bucket1"]["token"]), 1)
        self.assertEqual(len(annotations2["corpus1"]["bucket1"]["tokenwithlemma"]), 14)
        self.assertTrue("bucket2" not in annotations["corpus1"])

    def test_get_annotations_with_unexisting_document(self):
        ds = DocumentSearch(envIdReadOnly, authorizationReadOnly, "docNotExist", "corpus1")
        annotations = ds.get_annotations({"bucket1": ["token", "tokenwithlemma"]})
        self.assertFalse(annotations["corpus1"])

    def test_remove_all_remove_annotations(self):
        self.recreate_read_write_env()
        jsonSchema1 = {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "targetType": "document_surface1d",
            "schemaType": "schema1",
            "type": "object",
            "required": [
                "_schemaType",
                "_corpusID",
                "_documentID",
                "offsets"
            ],
            "properties": {
                "_schemaType": {
                    "type": "string",
                    "description": "Schema type",
                    "searchable": True,
                    "searchModes": ["noop"],
                    "locked": True
                },
                "_documentID": {
                    "type": "string",
                    "description": "Internal document GUID",
                    "searchable": True,
                    "searchModes": ["basic"],
                    "locked": True
                },
                "_corpusID": {
                    "type": "string",
                    "description": "Internal Corpus GUID",
                    "searchable": True,
                    "searchModes": ["basic"],
                    "locked": True
                }
            }
        }

        corpus = get_master_document_corpus_list(self.envId, self.authorization).create_corpus("corpusx")
        corpus.add_text_document("Another doc with auto id", "do1", "english")
        bucket1 = corpus.create_bucket("bucket1", "bucket1")
        schemaId1 = get_schema_list(self.envId, self.authorization).add_json_schema_as_hash(jsonSchema1, False, {})
        time.sleep(1)
        bucket1.add_or_update_schema_to_bucket(schemaId1, "schema1", TargetType("document_surface1d"), {})
        time.sleep(1)
        anno1 = {
            "_schemaType": "schema1",
            "_documentID": "document1",
            "_corpusID": "corpusx"

        }
        bucket1.add_annotation(anno1, "schema1", "1")
        time.sleep(1)
        documentSearch = DocumentSearch(self.envId, self.authorization, None, "corpusx")
        documentSearch.delete_annotations_for_types("bucket1", ["schema1"])
        time.sleep(1)
        bucket1.add_annotation(anno1, "schema1", "1")

    def tearDown(self):
        try:
            self.envList1.delete_env(self.envId)
        except:
            pass

    # we keep the search environement to speed up tests
    """
    @classmethod
    def tearDownClass(cls):
        try:
            authorization = BaseAuthorization("unitsearch_", None, None, None)
            envList1 = get_env_list(authorization)
            envList1.delete_env("unitsearch_")
        except:
            pass
    """


if __name__ == '__main__':
    unittest.main()
