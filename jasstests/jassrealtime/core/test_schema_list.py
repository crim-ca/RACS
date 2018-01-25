import unittest
import time

from jassrealtime.core.document_directory import DocumentDirectoryList
from jassrealtime.core.schema_list import *
from jassrealtime.core.settings_utils import get_language_manager

TEST_JSON_SCHEMA_STR = """
    {
        "$schema": "http://adnotare.crim.ca/schema/custom-meta-schema#",
        "id": "http://adnotare.crim.ca/schema/custom-meta-schema#",
        "description": "A representation of a face detection in a video media file",
        "type": "object",
        "required": [
            "@context",
            "@type",
            "_targetType",
            "_bucketID",
            "begin",
            "end",
            "confidence",
            "faceId",
            "poseType"
        ],
        "properties": {
            "@context": {
                "format": "url",
                "type": "string",
                "description": "JSON-LD context",
                "locked" : true,
                "searchable": true,
                "searchModes": ["noop"]
            },
            "@type": {
                "type": "string",
                "description": "JSON-LD type",
                "locked" : true,
                "searchable": true,
                "searchModes": ["noop"]
            },
            "_schemaType": {
              "type": "string",
                "description": "Internal schema ID",
                "locked": true,
                "searchable": true,
                "searchModes": ["noop"]
            },
            "_bucketID":{
                "type": "string",
                "description": "Bucket internal GUID",
                "locked" : true,
                "searchable": true,
                "searchModes": ["noop"]
            },
            "begin": {
                "format": "date-time-local",
                "type": "string",
                "description": "Annotation starting time",
                "locked" : true,
                "searchable": true,
                "searchModes": ["noop"]
            },
            "end": {
                "format": "date-time-local",
                "type": "string",
                "description": "Annotation ending time",
                "locked" : true,
                "searchable": true,
                "searchModes": ["noop"]
            },
            "confidence": {
                "maximum": "100",
                "minimum": "0",
                "type": "number",
                "searchable" : false,
                "locked" : true
            },
            "faceId": {
                "title": "Face Id",
                "description": "Face detection faceId",
                "type": "string",
                "language": "french",
                "searchable": true,
                "searchModes": ["noop"]
            },
            "poseType": {
                "title": "Pose Type",
                "description": "Face detection poseType",
                "type": "number",
                "enum": [
                    -2, -1, 0, 1, 2
                ],
                "default": 0,
                "searchable": true,
                "searchModes": ["noop"]
            }
        }
    }
    """
TEST_ES_PROPERTIES = '{"properties": {"end": {"type": "string"}, "_bucketID": {"type": "string"}, "@type": {"type": "string"}, "begin": {"type": "string"}, "poseType": {"type": "double"}, "_schemaType": {"type": "string"}, "@context": {"type": "string"}, "faceId": {"type": "string", "analyzer": "french"}, "confidence": {"type": "double", "index": "no"}}}'

JSON_SCHEMA_WITH_STRING_ARRAY = """
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "targetType": "document_surface1d",
  "schemaType": "TSD",
  "title": "TSD",
  "type": "object",
  "required": [
    "schemaType",
    "_corpusID",
    "_documentID",
    "offsets",
    "text",
    "conceptID",
    "domainNames"
  ],
  "properties": {
    "schemaType": {
      "type": "string",
      "description": "Constant: 'TSD' ",
      "searchable": true,
      "searchModes": [
        "noop"
      ],
      "default": "TSD",
      "locked": true
    },
    "_documentID": {
      "type": "string",
      "description": "Internal document GUID",
      "searchable": true,
      "searchModes": [
        "noop"
      ],
      "locked": true
    },
    "_corpusID": {
      "type": "string",
      "description": "Internal Corpus GUID",
      "searchable": true,
      "searchModes": [
        "noop"
      ],
      "locked": true
    },
    "offsets": {
      "type": "array",
      "searchable": true,
      "searchModes": [
        "noop"
      ],
      "locked": true,
      "description": "Position of the token within the target document",
      "minItems": 1,
      "maxItems": 1,
      "items": {
        "type": "object",
        "properties": {
          "begin": {
            "type": "integer",
            "minimum": 0
          },
          "end": {
            "type": "integer",
            "minimum": 0
          }
        }
      }
    },
    "text": {
      "type": "string",
      "description": "Targeted text",
      "searchable": true,
      "searchModes": ["basic"],
      "locked": false
    },
    "domainNames": {
      "type": "array",
      "searchable": true,
      "searchModes": ["basic"],
      "locked": true,
      "description": "Domain Names associated with the term",
      "minItems": 1,
      "items": {
        "type": "string"
      }
    },
    "conceptID": {
      "type": "string",
      "description": "ID of the concept associated with the term",
      "searchable": true,
      "searchModes": ["noop"],
      "locked": true
    },
    "conceptDescription": {
      "type": "string",
      "description": "Description of the concept associated with the term",
      "searchable": true,
      "searchModes": ["basic"],
      "locked": true
    },
    "gender": {
      "type": "string",
      "description": "Gender of the term",
      "searchable": true,
      "searchModes": ["noop"],
      "locked": true
    },
    "number": {
      "type": "string",
      "description": "Number of the term",
      "searchable": true,
      "searchModes": ["noop"],
      "locked": true
    },
    "category": {
      "type": "string",
      "description": "Part of speech of the term",
      "searchable": true,
      "searchModes": ["noop"],
      "locked": true
    },
    "score": {
      "type": "number",
      "description": "Score associated with the concept",
      "searchable": true,
      "searchModes": ["noop"],
      "locked": true
    }
  }
}"""


class MyTestCase(unittest.TestCase):
    def setUp(self):
        try:
            setting = get_settings()
            self.envId = "unittest"
            self.authorization = BaseAuthorization(self.envId, None, None, None)
            self.masterList = DocumentDirectoryList.create(self.envId, setting['CLASSES']['DOCUMENT_DIRECTORY'],
                                                           self.authorization)
            self.schemaList = SchemaList.create(self.envId, setting['CLASSES']['SCHEMA_LIST'], self.authorization,
                                                get_language_manager())

        finally:
            pass

    def test_convert_json_schema_to_es_properties(self):
        jsonSchema = json.loads(TEST_JSON_SCHEMA_STR)
        esSchema = self.schemaList.convert_json_schema_to_es_properties(jsonSchema)
        str = json.dumps(esSchema)

    def test_add_es_schema(self):
        esProperties = json.loads(TEST_ES_PROPERTIES)
        esHash1 = self.schemaList.add_es_schema(esProperties)
        esHash2 = self.schemaList.add_es_schema(esProperties)
        self.assertEqual(esHash1, esHash2)

    def test_add_json_schema(self):
        jsonSchema = json.loads(TEST_JSON_SCHEMA_STR)
        id = self.schemaList.add_json_schema(jsonSchema)
        time.sleep(1)
        schemasInfo = self.schemaList.get_json_schemas_infos()
        self.assertEqual(1, len(schemasInfo))
        # adding a name
        id = self.schemaList.add_json_schema(jsonSchema, "Anton Schema")
        time.sleep(1)
        schemasInfo = self.schemaList.get_json_schemas_infos()
        self.assertEqual(2, len(schemasInfo))

    def test_add_json_schema_string_array(self):
        jsonSchema = json.loads(JSON_SCHEMA_WITH_STRING_ARRAY)
        self.schemaList.add_json_schema(jsonSchema, nestedFields=["offsets"])
        time.sleep(1)
        schemasInfo = self.schemaList.get_json_schemas_infos()
        self.assertEqual(1, len(schemasInfo))
        # adding a name

    def test_add_json_schema_as_hash(self):
        authorization = BaseAuthorization.create_authorization(self.envId, None, None)
        jsonSchema = json.loads(TEST_JSON_SCHEMA_STR)
        id1 = self.schemaList.add_json_schema_as_hash(jsonSchema)
        time.sleep(1)
        # adding a name
        id2 = self.schemaList.add_json_schema_as_hash(jsonSchema)
        time.sleep(1)
        self.assertEqual(id1, id2)
        # should only have 1 schema
        schemasInfo = self.schemaList.get_json_schemas_infos()
        self.assertEqual(1, len(schemasInfo))

    def test_add_json_schema_with_offsets(self):
        jsonSchema = """
        {
              "$schema": "http://json-schema.org/draft-04/schema#",
              "targetType": "document_surface1d",
              "schemaType": "document_surface1d",
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
                  "searchModes": ["noop"]
                  "locked": true
                },
                "_documentID": {
                  "type": "string",
                  "description": "Internal document GUID",
                  "searchable": true,
                  "searchModes": ["basic"],
                  "locked": true
                },
                "_corpusID": {
                  "type": "string",
                  "description": "Internal Corpus GUID",
                  "searchable": true,
                  "searchModes": ["basic"],
                  "locked": true
                },
                "offsets": {
                  "searchable": true,
                  "locked": true,
                  "type": "array",
                  "minItems": 1,
                  "items": {
                    "type": "object",
                    "properties": {
                        "begin": { "type": "integer", "minimum": 0},
                        "end": { "type": "integer", "minimum": 0}
                    }
                  }
                }
              }
            }
        """
        authorization = BaseAuthorization.create_authorization(self.envId, None, None)
        jsonSchema = json.loads(TEST_JSON_SCHEMA_STR)
        id1 = self.schemaList.add_json_schema_as_hash(jsonSchema, False, ["offsets"])

    def test_get_json_schemas(self):
        authorization = BaseAuthorization.create_authorization(self.envId, None, None)
        jsonSchema = json.loads(TEST_JSON_SCHEMA_STR)
        id = self.schemaList.add_json_schema(jsonSchema)
        id = self.schemaList.add_json_schema(jsonSchema, "Anton Schema")
        id = self.schemaList.add_json_schema(jsonSchema, "Fred Schema", "Internal Use Only")
        time.sleep(1)
        antonSchemas = self.schemaList.get_json_schemas_infos(name="Anton")
        self.assertEqual(1, len(antonSchemas))
        jsonSchemaHash = antonSchemas[0]["jsonSchemaHash"]
        sameJsonHashSchemas = self.schemaList.get_json_schemas_infos(jsonSchemaHash=jsonSchemaHash)
        self.assertEqual(3, len(sameJsonHashSchemas))

    def test_can_migrate(self):
        JsonSchema1 = """
            {
                "properties": {
                    "@context": {
                        "format": "url",
                        "type": "string",
                        "description": "JSON-LD context",
                        "locked" : true
                    },
                    "@type": {
                        "type": "string",
                        "description": "JSON-LD type",
                        "locked" : true
                    }
                }
            }
            """
        JsonSchema2 = """
            {
                "properties": {
                    "@context": {
                        "format": "url",
                        "type": "string",
                        "description": "JSON-LD context",
                        "locked" : true
                    },
                    "@type": {
                        "type": "string",
                        "description": "JSON-LD type",
                        "locked" : true
                    },
                    "_schemaType": {
                      "type": "string",
                        "description": "Internal schema ID",
                        "locked": true
                    }
                }
            }
            """
        JsonSchema3 = """
            {
                "properties": {
                    "@context": {
                        "format": "url",
                        "type": "string",
                        "description": "JSON-LD context",
                        "locked" : true
                    },
                    "@type": {
                        "type": "integer",
                        "locked" : true
                    },
                    "_schemaType": {
                      "type": "string",
                        "description": "Internal schema ID",
                        "locked": true
                    }
                }
            }
        """
        authorization = BaseAuthorization.create_authorization(self.envId, None, None)
        time.sleep(1)
        self.schemaList.add_json_schema(json.loads(JsonSchema1), "Schema1")
        time.sleep(1)
        self.schemaList.add_json_schema(json.loads(JsonSchema2), "Schema2")
        time.sleep(1)
        id3 = self.schemaList.add_json_schema(json.loads(JsonSchema3), "Schema3")
        time.sleep(1)
        esHash1 = self.schemaList.get_json_schemas_infos("Schema1")[0]["esHash"]
        esHash2 = self.schemaList.get_json_schemas_infos("Schema2")[0]["esHash"]
        esHash3 = self.schemaList.get_json_schema_info(id3)["esHash"]
        esHash1Prop = self.schemaList.get_es_properties(esHash1)
        esHash2Prop = self.schemaList.get_es_properties(esHash2)
        esHash3Prop = self.schemaList.get_es_properties(esHash3)

        self.assertTrue(SchemaList.can_migrate(esHash1Prop, esHash2Prop))
        self.assertFalse(SchemaList.can_migrate(esHash1Prop, esHash3Prop))
        self.assertRaises(EsSchemaMigrationDeleteFieldsNotSupportedException,
                          SchemaList.can_migrate, esHash2Prop, esHash1Prop)

    def test_hash_es_properties(self):
        # Theoretically sorted. Good for debugging
        '''
        {
            "properties": {
                "b": {"arr": [1, 2, 3], "type": "string"},
                "c": { "a": {"arr2": ["a","c"],"b": "c", "d": "d"},"type": "integer"},
                "d": "test"
            }
        }
        '''

        properties1 = {
            "properties": {
                "b": {"type": "string", "arr": [1, 2, 3]},
                "c": {"type": "integer", "a": {"b": "c", "arr2": ["c", "a"], "d": "d"}},
                "d": "test"
            }
        }

        properties2 = {
            "properties": {
                "d": "test",
                "c": {"type": "integer", "a": {"arr2": ["c", "a"], "b": "c", "d": "d"}},
                "b": {"arr": [3, 2, 1], "type": "string"}
            }
        }
        self.assertEqual(SchemaList.hash_es_properties(properties1), SchemaList.hash_es_properties(properties2))

    def tearDown(self):
        try:
            es = get_es_conn()
            self.schemaList.delete()
            self.masterList.delete()

        finally:
            pass

    @classmethod
    def tearDownClass(cls):
        es = get_es_conn()
        es_wait_ready()
        es.indices.delete(index="unittest_*")
        es_wait_ready()
