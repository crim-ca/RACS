import unittest
import os

from jassrealtime.core.settings_utils import get_settings, set_setting_path
from jassrealtime.document.document_corpus import *
from jassrealtime.document.bucket import *
from jassrealtime.core.master_factory_list import get_schema_list, get_env_list, \
    get_master_document_corpus_list
from jassrealtime.core.env import EnvAlreadyExistWithSameIdException
from jasstests.jassrealtime.core.test_schema_list import JSON_SCHEMA_WITH_STRING_ARRAY


class MyTestCase(unittest.TestCase):
    def setUp(self):
        try:
            setting = get_settings()
            self.envId = "unittest_"
            self.authorization = BaseAuthorization("unittest_", None, None, None)
            self.envList1 = get_env_list(self.authorization)
            try:
                self.envList1.create_env(self.envId)
            except EnvAlreadyExistWithSameIdException:
                time.sleep(1)
                self.envList1.delete_env(self.envId)
                self.envList1.create_env(self.envId)
        finally:
            pass

    def test_get_schemas_info(self):
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

        jsonSchema2 = {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "targetType": "document_surface1d",
            "schemaType": "schema2",
            "type": "object",
            "required": [
                "_schemaType",
            ],
            "properties": {
                "_schemaType": {
                    "type": "string",
                    "description": "Schema type",
                    "searchable": True,
                    "searchModes": ["noop"],
                    "locked": True
                }
            }
        }

        corpus = get_master_document_corpus_list(self.envId, self.authorization).create_corpus("corpus1")
        bucket1 = corpus.create_bucket("bucket1")
        schemaId1 = get_schema_list(self.envId, self.authorization).add_json_schema_as_hash(jsonSchema1, False, {})
        schemaId2 = get_schema_list(self.envId, self.authorization).add_json_schema_as_hash(jsonSchema2, False, {})
        time.sleep(1)

        bucket1.add_or_update_schema_to_bucket(schemaId1, "schema1", TargetType("document"), {})
        bucket1.add_or_update_schema_to_bucket(schemaId2, "schema2", TargetType("document"), {})
        time.sleep(1)
        res = bucket1.get_schemas_info(True)
        self.assertEqual(len(res["data"]), 2)

    def test_bind_schema_with_string_array(self):
        schema = json.loads(JSON_SCHEMA_WITH_STRING_ARRAY)

        corpus = get_master_document_corpus_list(self.envId, self.authorization).create_corpus("corpus1")
        bucket1 = corpus.create_bucket("bucket1")
        schema_id = get_schema_list(self.envId, self.authorization).add_json_schema_as_hash(schema, False,
                                                                                            nestedFields=["offsets"])
        time.sleep(1)

        bucket1.add_or_update_schema_to_bucket(schema_id, "schema1", TargetType("document"), {})
        time.sleep(1)
        res = bucket1.get_schemas_info(True)
        self.assertEqual(len(res["data"]), 1)

    def test_delete_schema_type(self):
        jsonSchema1 = {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "targetType": "document_surface1d",
            "schemaType": "schema1",
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Internal document GUID",
                    "searchable": True,
                    "searchModes": ["basic"],
                    "locked": True
                }
            }
        }

        jsonSchema2 = {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "targetType": "document_surface1d",
            "schemaType": "schema2",
            "type": "object",
            "properties": {
                "city": {
                    "type": "string",
                    "description": "Internal document GUID",
                    "searchable": True,
                    "searchModes": ["basic"],
                    "locked": True
                }

            }
        }

        corpus = get_master_document_corpus_list(self.envId, self.authorization).create_corpus("corpus1")
        bucket1 = corpus.create_bucket("bucket1")
        schemaId1 = get_schema_list(self.envId, self.authorization).add_json_schema_as_hash(jsonSchema1, False, {})
        schemaId2 = get_schema_list(self.envId, self.authorization).add_json_schema_as_hash(jsonSchema2, False, {})
        time.sleep(1)

        bucket1.add_or_update_schema_to_bucket(schemaId1, "schema1", TargetType("document"), {})
        bucket1.add_or_update_schema_to_bucket(schemaId2, "schema2", TargetType("document"), {})
        bucket1.add_annotation({"name": "Anton"}, "schema1", "1")
        bucket1.add_annotation({"name": "JF"}, "schema1", "2")
        bucket1.add_annotation({"city": "Montreal"}, "schema2", "1")
        bucket1.add_annotation({"city": "Quebec"}, "schema2", "2")
        time.sleep(1)

        anno1 = bucket1.get_annotation("1", "schema1")

        bucket1.delete_schema_type("schema1")
        time.sleep(1)

        info = bucket1.get_schemas_info()
        self.assertEqual(len(info["data"]), 1)
        # making shure the annotation remains in schema 2
        anno1 = bucket1.get_annotation("1", "schema2")
        self.assertEqual(anno1["city"], "Montreal")
        anno2 = bucket1.get_annotation("2", "schema2")
        self.assertEqual(anno2["city"], "Quebec")

        # create the same schema type but with different data
        bucket1.add_or_update_schema_to_bucket(schemaId1, "schema1", TargetType("document"), {})
        bucket1.add_annotation({"name": "Yolo"}, "schema1", "3")
        bucket1.add_annotation({"name": "Rage"}, "schema1", "4")
        time.sleep(1)
        # make sure old annotations do not exists
        anno1 = bucket1.get_annotation("3", "schema1")
        self.assertEqual(anno1["name"], "Yolo")
        anno2 = bucket1.get_annotation("4", "schema1")
        self.assertEqual(anno2["name"], "Rage")
        self.assertRaises(DocumentNotFoundException, bucket1.get_annotation, "1", "schema1")

    def tearDown(self):
        try:
            self.envList1.delete_env(self.envId)
        except:
            pass

    @classmethod
    def tearDownClass(cls):
        try:
            authorization = BaseAuthorization("unittest_", None, None, None)
            envList1 = get_env_list(authorization)
            envList1.delete_env("unittest_")
        except:
            pass


if __name__ == '__main__':
    unittest.main()
