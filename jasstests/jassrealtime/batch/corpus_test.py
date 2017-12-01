import unittest
import os,shutil,glob

from jassrealtime.core.settings_utils import get_settings, set_setting_path
from jassrealtime.document.document_corpus import *
from jassrealtime.security.security_selector import get_autorisation
from jassrealtime.document.bucket import *
from jassrealtime.core.master_factory_list import get_schema_list, get_env_list,\
    get_master_document_corpus_list, get_schema_list,get_master_bucket_list
from jassrealtime.core.env import EnvAlreadyExistWithSameIdException
from jassrealtime.batch.corpus import *
from jassrealtime.batch.tmp_file_storage import TmpFileStorage


SCHEMA_NORMAL = {
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

SCHEMA_OFFSETS = {
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

JASS_TEST_DATA_PATH = os.path.join("/tmp","jass_test_data")

class MyTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # make a tmp dir and copy all test data files into it.
        if os.path.exists(JASS_TEST_DATA_PATH):
            shutil.rmtree(JASS_TEST_DATA_PATH)
        os.makedirs(JASS_TEST_DATA_PATH)
        scriptPath = os.path.abspath(__file__)
        scriptDataDir = os.path.join(os.path.dirname(scriptPath), "data")
        files = glob.iglob(os.path.join(scriptDataDir, "*.zip"))
        for file in files:
            if os.path.isfile(file):
                shutil.copy2(file, JASS_TEST_DATA_PATH)

    def setUp(self):
        try:
            setting = get_settings()
            self.envId = "unittest_"
            self.authorization = BaseAuthorization("unittest_", None, None, None)
            self.envList1 = get_env_list(self.authorization)

            try:
                self.envList1.create_env(self.envId)
                pass
            except EnvAlreadyExistWithSameIdException:
                time.sleep(1)
                self.envList1.delete_env(self.envId)
                self.envList1.create_env(self.envId)
        finally:
            pass

    def get_zip_file_path(self,zipFileName:str):
        return os.path.join(JASS_TEST_DATA_PATH,zipFileName)

    def set_up_corpus(self):
        corpus = get_master_document_corpus_list(self.envId, self.authorization).create_corpus("corpus1")
        time.sleep(1)
        bucket1 = corpus.create_bucket("bucket1","bucket1")
        setting = get_settings()
        self.schemaList = get_schema_list(self.envId,self.authorization)
        schemaNormalId = self.schemaList.add_json_schema_as_hash(SCHEMA_NORMAL)
        schemaOffsetsId = self.schemaList.add_json_schema_as_hash(SCHEMA_OFFSETS,False,nestedFields=["offsets"])
        time.sleep(1)
        bucket1.add_or_update_schema_to_bucket(schemaNormalId, "sentence", TargetType.document_surface1d, {})
        bucket1.add_or_update_schema_to_bucket(schemaOffsetsId, "token", TargetType.document_surface1d, {})
        time.sleep(1)

    def test_add_annotations(self):
        self.set_up_corpus()
        batchCorpus = Corpus(self.envId, self.authorization, "corpus1")
        errors = batchCorpus.add_annotations("bucket1",self.get_zip_file_path("all_in_one.zip"))
        self.assertFalse(errors,str(errors))

    def test_update_annotations_multifiles(self):
        self.set_up_corpus()
        batchCorpus = Corpus(self.envId, self.authorization, "corpus1")
        errorsA = batchCorpus.add_annotations("bucket1", self.get_zip_file_path("multiple_json.zip"))
        self.assertFalse(errorsA, str(errorsA))
        time.sleep(0.1)
        errorsB = batchCorpus.add_annotations("bucket1", self.get_zip_file_path("update_token_only.zip"))
        self.assertFalse(errorsB, str(errorsB))
        time.sleep(0.1)
        bucket1 = get_master_bucket_list(self.envId, self.authorization).get_bucket("corpus1","bucket1")
        annotation = bucket1.get_annotation("t1","token")
        self.assertEqual(annotation["category"],"UPDATED")

    def test_invalid_annotation(self):
        self.set_up_corpus()
        batchCorpus = Corpus(self.envId, self.authorization, "corpus1")
        errors = batchCorpus.add_annotations("bucket1", self.get_zip_file_path("one_invalid_token.zip"))
        self.assertTrue(errors,str(errors))
        time.sleep(0.1)
        bucket1 = get_master_bucket_list(self.envId, self.authorization).get_bucket("corpus1", "bucket1")
        # Last annotation, needs to exist, since elastic search
        annotation = bucket1.get_annotation("token_last", "token")

    def tearDown(self):
        try:
            pass
            #self.envList1.delete_env(self.envId)
        except:
            pass

    @classmethod
    def tearDownClass(cls):
        try:
            authorization = BaseAuthorization("unittest_", None, None, None)
            envList1 = get_env_list(authorization)
            #envList1.delete_env("unittest_")
        except:
            pass


if __name__ == '__main__':
    unittest.main()
