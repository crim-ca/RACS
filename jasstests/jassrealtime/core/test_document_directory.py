import unittest

from jassrealtime.core.document_directory import *
from jassrealtime.core.esutils import es_wait_ready
from jassrealtime.core.esutils import get_es_conn
from jassrealtime.core.settings_utils import *


class MyTestCase(unittest.TestCase):
    def setUp(self):
        setting = get_settings()
        self.envId = "unittest"
        sett = setting['CLASSES']['DOCUMENT_DIRECTORY']
        self.authorization = BaseAuthorization(self.envId, None, None, None)
        self.masterList = DocumentDirectoryList.create("unittest", sett, self.authorization)

    def test_string_to_id(self):
        self.assertEqual("_-9a1", string_to_id("  _*&^%&^ -9 a1"))

    def test_create_duplicate_document_directory_exception(self):
        es = get_es_conn()
        dd = self.masterList.create_document_directory("dup1")
        self.assertRaises(DocumentDirectoryAlreadyExistsException, self.masterList.create_document_directory, "dup1")
        res = self.masterList.get_directories()
        self.assertEqual(True, True)

    def test_create_duplicate_document_directory__alias_exception(self):
        es = get_es_conn()
        dd = self.masterList.create_document_directory("dup2", "aliasdup")
        self.assertRaises(DocumentDirectoryAliasAlreadyExistsException, self.masterList.create_document_directory,
                          "dup3", "aliasdup")
        res = self.masterList.get_directories()
        self.assertEqual(True, True)

    def test_document_exists(self):
        es = get_es_conn()
        dd = self.masterList.create_document_directory("docs")
        body = {"name": "anton", "age": 666}
        dd.add_document(body, 1, "type1")
        dd.add_document(body, 2, "type2")
        time.sleep(1)
        self.assertTrue(dd.document_exist(1, "type1"))
        self.assertFalse(dd.document_exist(1, "type2"))
        self.assertTrue(dd.document_exist(2, "type2"))
        self.assertTrue(dd.document_exist(2, None))
        self.assertFalse(dd.document_exist(3, None))

    def test_create_default_document(self):
        es = get_es_conn()
        dd = self.masterList.create_document_directory("docs")
        body = {"name": "anton", "age": 666}
        dd.add_document(body, "_9XVI-_Ac")
        # get some time for es to index the document
        time.sleep(1)
        res = dd.get_document("_9XVI-_Ac")
        self.assertEqual(res["name"], body["name"])

    def test_create_document_do_dynamic_mappings(self):
        es = get_es_conn()
        dd = self.masterList.create_document_directory("docs")
        body = {"name": "anton", "age": 666}
        dd._create_data_index_if_not_exist("person",False)
        self.assertRaises(DocumentDoesNotRespectSchemaException, dd.add_document,body, "_9XVI-_Ac","person")

    def test_create_document_no_id(self):
        es = get_es_conn()
        dd = self.masterList.create_document_directory("docs")
        body = {"name": "anton", "age": 666}
        dd.add_document(body, None, "person")

    def test_create_document_no_default_type(self):
        es = get_es_conn()
        dd = self.masterList.create_document_directory("docs",None,False)
        body = {"name": "anton", "age": 666}
        dd.add_document(body,None)

    def test_document_not_found_exception(self):
        es = get_es_conn()
        dd = self.masterList.create_document_directory("docs")
        body = {"name": "anton", "age": 666}
        dd.add_document(body, "1", "person")
        time.sleep(1)
        self.assertRaises(DocumentNotFoundException, dd.get_document, "2", "person")
        self.assertRaises(DocumentNotFoundException, dd.get_document, "1", "test2")

    def test_incompatible_schema_for_document_type(self):
        es = get_es_conn()
        dd = self.masterList.create_document_directory("docs")
        body1 = {"name": "anton", "age": 666}
        mapping1 = {
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"}
            }
        }
        dd.add_document(body1, None, "person", mapping1)
        body2 = {"name": "anton", "age": "I AM A RETARD"}
        mapping2 = {
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "string"}
            }
        }
        time.sleep(1)
        self.assertRaises(EsSchemaMigrationInvalidException, dd.add_document, body2, None, "person", mapping2)

    def test_compatible_schema_for_document_type(self):
        es = get_es_conn()
        dd = self.masterList.create_document_directory("docs")
        body1 = {"name": "anton", "age": 666}
        mapping1 = {
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"}
            }
        }
        dd.add_document(body1, None, "person", mapping1)
        body2 = {"name": "anton", "age": 777}
        mapping2 = {
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"}
            }
        }
        time.sleep(1)
        dd.add_document(body2, None, "person", mapping2)

    def test_document_update(self):
        es = get_es_conn()
        dd = self.masterList.create_document_directory("docs")
        mapping1 = {
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"}
            }
        }
        body1 = {"name": "anton", "age": 666}
        dd.add_document(body1, 1, "person", mapping1)
        time.sleep(1)
        body2 = {"name": "anton", "age": 777}
        dd.update_document(body2, 1, "person")
        time.sleep(1)
        self.assertEqual(777, dd.get_document(1, "person")["age"])

    def test_document_add_duplicate_exception(self):
        es = get_es_conn()
        dd = self.masterList.create_document_directory("docs")
        body1 = {"name": "anton", "age": 666}
        dd.add_document(body1, 1, "person")
        time.sleep(1)
        body2 = {"name": "anton", "age": 777}
        self.assertRaises(DocumentAlreadyExistsException, dd.add_document, body2, 1, "person")

    def test_document_exist(self):
        es = get_es_conn()
        dd = self.masterList.create_document_directory("docs")
        body = {"name": "anton", "age": 666}
        dd.add_document(body, "1", "person")
        time.sleep(1)
        self.assertTrue(dd.document_exist("1", "person"))
        self.assertFalse(dd.document_exist("2", "person"))

    def test_small_search_lots(self):
        es = get_es_conn()
        dd = self.masterList.create_document_directory("docs")
        propertiesMyTermMapping = {
            "properties": {
                "age": {
                    "type": "integer",
                    "index": "not_analyzed"
                },
                "name": {
                    "type": "string",
                }
            }
        }
        for i in range(0,100):
            dd.add_document({"name": "Anton", "age": i}, i, "person", propertiesMyTermMapping)

        for i in range(0, 50):
            dd.add_document({"name": "JF", "age": i}, i+100, "person", propertiesMyTermMapping)

        time.sleep(2)
        allJFs = dd.small_search(docTypes=['person'],matchFields={"name": "JF"},useScan=False)
        self.assertEqual(len(allJFs), 50)

        # testing with scan
        allJFs = dd.small_search(docTypes=['person'], matchFields={"name": "JF"}, useScan=True)
        self.assertEqual(len(allJFs), 50)


    def test_small_search_terms(self):
        es = get_es_conn()
        dd = self.masterList.create_document_directory("docs")
        propertiesMyTermMapping = {
            "properties": {
                "myterm": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "age": {
                    "type": "integer",
                    "index": "not_analyzed"
                },
                "location": {
                    "type": "string",
                },
                "name": {
                    "type": "string",
                }
            }
        }
        dd.add_document({"myterm": "Anton", "name": "Anton", "age": 28}, 1, "person", propertiesMyTermMapping)
        dd.add_document({"myterm": "Berlin", "name": "Berlin", "age": 24}, 2, "person", propertiesMyTermMapping)
        dd.add_document({"myterm": "Anton Mansion", "name": "Anton Mansion", "location": "666 Europe Street"}, 3,
                        "house", propertiesMyTermMapping)
        dd.add_document({"myterm": "Berlin Mansion", "name": "Berlin Mansion", "location": "Europe"}, 4, "house",
                        propertiesMyTermMapping)
        dd.add_document({"myterm": "Berlin", "name": "Berlin", "location": "Europe East"}, 5, "city",
                        propertiesMyTermMapping)
        dd.add_document({"myterm": "Berlin West", "name": "Berlin", "location": "Europe West"}, 6, "city",
                        propertiesMyTermMapping)
        time.sleep(1)
        city_myterm_Berlin = dd.small_search(docTypes=["city"], termFields={"myterm": "Berlin"})
        self.assertEqual(len(city_myterm_Berlin), 1)
        self.assertEqual(city_myterm_Berlin[0]["id"], "5")

        location_europe_city_match = dd.small_search(docTypes=["city"], matchFields={"name": "Berlin"})
        self.assertEqual(len(location_europe_city_match), 2)

        name_Berlin_match_all = dd.small_search(docTypes=[], matchFields={"name": "Berlin"})
        self.assertEqual(len(name_Berlin_match_all), 4)

        name_Berlin_match_all_except_houses = dd.small_search(docTypes=["person", "city", ],
                                                              matchFields={"name": "Berlin"})
        self.assertEqual(len(name_Berlin_match_all_except_houses), 3)

        term_Berlin_location_europe_all = dd.small_search(docTypes=[], termFields={"myterm": "Berlin"},
                                                          matchFields={"location": "Europe"})
        self.assertEqual(len(term_Berlin_location_europe_all), 1)

        term_Berlin_name_only_match_city_person = dd.small_search(docTypes=["city", "person"],
                                                                  termFields={"myterm": "Berlin"},
                                                                  returnFields=["name"])
        self.assertEqual(len(term_Berlin_name_only_match_city_person), 2)
        self.assertTrue(not "location" in term_Berlin_name_only_match_city_person[0])

        all_ids_types = dd.small_search(docTypes=[], returnFields=[])
        self.assertEqual(len(all_ids_types), 6)

    def test_small_search_terms_no_default_schema(self):
        es = get_es_conn()
        dd = self.masterList.create_document_directory("docs",None,False)
        propertiesMyTermMapping = {
            "properties": {
                "myterm": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "age": {
                    "type": "integer",
                    "index": "not_analyzed"
                },
                "location": {
                    "type": "string",
                },
                "name": {
                    "type": "string",
                }
            }
        }
        dd.add_document({"myterm": "Anton", "name": "Anton", "age": 28}, 1, "person", propertiesMyTermMapping)
        dd.add_document({"myterm": "Berlin", "name": "Berlin", "age": 24}, 2, "person", propertiesMyTermMapping)
        dd.add_document({"myterm": "Anton Mansion", "name": "Anton Mansion", "location": "666 Europe Street"}, 3,
                        "house", propertiesMyTermMapping)
        dd.add_document({"myterm": "Berlin Mansion", "name": "Berlin Mansion", "location": "Europe"}, 4, "house",
                        propertiesMyTermMapping)
        dd.add_document({"myterm": "Berlin", "name": "Berlin", "location": "Europe East"}, 5, "city",
                        propertiesMyTermMapping)
        dd.add_document({"myterm": "Berlin West", "name": "Berlin", "location": "Europe West"}, 6, "city",
                        propertiesMyTermMapping)
        time.sleep(1)
        city_myterm_Berlin = dd.small_search(docTypes=["city"], termFields={"myterm": "Berlin"})
        self.assertEqual(len(city_myterm_Berlin), 1)
        self.assertEqual(city_myterm_Berlin[0]["id"], "5")

        location_europe_city_match = dd.small_search(docTypes=["city"], matchFields={"name": "Berlin"})
        self.assertEqual(len(location_europe_city_match), 2)

        name_Berlin_match_all = dd.small_search(docTypes=[], matchFields={"name": "Berlin"})
        self.assertEqual(len(name_Berlin_match_all), 4)

        name_Berlin_match_all_except_houses = dd.small_search(docTypes=["person", "city", ],
                                                              matchFields={"name": "Berlin"})
        self.assertEqual(len(name_Berlin_match_all_except_houses), 3)

        term_Berlin_location_europe_all = dd.small_search(docTypes=[], termFields={"myterm": "Berlin"},
                                                          matchFields={"location": "Europe"})
        self.assertEqual(len(term_Berlin_location_europe_all), 1)

        term_Berlin_name_only_match_city_person = dd.small_search(docTypes=["city", "person"],
                                                                  termFields={"myterm": "Berlin"},
                                                                  returnFields=["name"])
        self.assertEqual(len(term_Berlin_name_only_match_city_person), 2)
        self.assertTrue(not "location" in term_Berlin_name_only_match_city_person[0])

        all_ids_types = dd.small_search(docTypes=[], returnFields=[])
        self.assertEqual(len(all_ids_types), 6)

        # should return both person and house.

    '''
    def create_delete_document_directory(self):
        """
        :return:
        """
        # DocumentsDirectory
        #create_document_directory("testdirectory1")

    '''

    def test_delete_doc_type(self):
        es = get_es_conn()
        dd = self.masterList.create_document_directory("docs")
        mapping1 = {
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"}
            }
        }
        # create a doc type person.
        body1 = {"name": "anton", "age": 666}
        dd.add_document(body1, 1, "person", mapping1)

        # create a doc type animal.
        body2 = {"name": "doggy", "age": 3}
        dd.add_document(body2, 1, "animal", mapping1)
        time.sleep(1)

        # erase type animal
        dd.delete_doc_type("animal")
        time.sleep(1)

        self.assertFalse("animal" in dd.get_indices_per_doc_type(),"Indexes found for the deleted animal index")

        self.assertFalse(dd.document_exist(1, "animal"), "Animal found but animal index is supposed to be deleted")
        self.assertTrue(dd.document_exist(1, "person"), "Person not found, but person index is suppsed to exist")


    def test_empty_doc_type(self):
        es = get_es_conn()
        dd = self.masterList.create_document_directory("docs")
        mapping1 = {
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"}
            }
        }
        # create a doc type person.
        body1 = {"name": "anton", "age": 666}
        dd.add_document(body1, 1, "person", mapping1)

        # create a doc type animal.
        body2 = {"name": "doggy", "age": 3}
        dd.add_document(body2, 1, "animal", mapping1)
        time.sleep(1)

        # erase type animal
        dd.empty_doc_type("animal")
        time.sleep(1)

        self.assertFalse(dd.document_exist("1", "animal"), "Animal found but animal index is supposed to be empty")

        body3 = {"name": "kitty", "age": 7}
        dd.add_document(body3, 2, "animal", mapping1)
        time.sleep(1)

        self.assertTrue(dd.document_exist(2, "animal"), "Animal was supposed to be added")
        self.assertTrue(dd.document_exist(1, "person"), "Person not found, but person index is suppsed to exist")



    def tearDown(self):
        es = get_es_conn()
        es_wait_ready()
        self.masterList.delete()
        es_wait_ready()

    @classmethod
    def tearDownClass(cls):
        set_setting_path(None)
        es = get_es_conn()
        es_wait_ready()
        # Main reason we dont read this from settings is to be sure not to wipe prod indexes.
        es.indices.delete(index="unittest_*")
        es_wait_ready()


if __name__ == '__main__':
    unittest.main()
