import unittest
import os

from jassrealtime.core.settings_utils import get_settings, set_setting_path
from jassrealtime.document.document_corpus import *


class MyTestCase(unittest.TestCase):
    def setUp(self):
        es = get_es_conn()
        es.indices.delete(index="unittest_*")
        time.sleep(0.1)
        setting = get_settings()
        self.envId = "unittest_"
        self.authorization = BaseAuthorization.create_authorization(self.envId, None, None)
        self.masterList = DocumentDirectoryList.create(self.envId, setting['CLASSES']['DOCUMENT_DIRECTORY'],
                                                       self.authorization)
        self.bucketList = BucketList.create(self.envId, setting['CLASSES']['BUCKET'], self.authorization)
        self.documentCorpusList = DocumentCorpusList.create(self.envId, setting['CLASSES']['DOCUMENT_CORPUS'],
                                                            self.authorization)

    def test_create_corpus(self):
        corpus = self.documentCorpusList.create_corpus()
        time.sleep(0.1)
        self.documentCorpusList.get_corpus(corpus.id)

    def test_create_modify_languages(self):
        # ISO language OK
        c1 = self.documentCorpusList.create_corpus(languages=["fr_FR"])
        self.assertRaises(CorpusInvalidFieldException, self.documentCorpusList.create_corpus, languages=None)
        self.assertRaises(CorpusInvalidFieldException, self.documentCorpusList.create_corpus,
                          languages=["doesnotexist"])

        # modifideleting now allowed
        c1 = self.documentCorpusList.create_corpus(id="c1", languages=["french", "english"])
        time.sleep(0.1)
        self.assertRaises(CorpusInvalidFieldException, self.documentCorpusList.update_corpus, id="c1",
                          languages=["french"])
        self.assertRaises(CorpusInvalidFieldException, self.documentCorpusList.update_corpus, id="c1",
                          languages=["french", "english", "doesnotexist"])

    def test_add_document(self):
        corpus = self.documentCorpusList.create_corpus()
        corpus.add_text_document("Life is gud when you are rich", "doc1", "english", 1)
        corpus.add_text_document("Life is bad when you are poor", "doc2", "english", 2)
        corpus.add_text_document("Another doc with auto id", "doc whaever", "english")
        corpus.add_text_document("Another doc whatever", "doc3", "english", 3)
        corpus.add_text_document("Another doc whatever++", "doc4", "english", 4)
        time.sleep(1)
        self.assertEqual(corpus.get_documents_count(), 5)
        self.assertRaises(CorpusDoesntContainLanguageException, corpus.add_text_document,
                          "Another doc with custom mapping", 5, "french")
        self.assertEqual(corpus.get_documents_count(), 5)
        doc = corpus.get_text_document("3")
        self.assertEqual(doc["text"], "Another doc whatever")
        self.assertRaises(DocumentNotFoundException, corpus.get_text_document, 10)

    def test_get_documents(self):
        corpus = self.documentCorpusList.create_corpus()
        time.sleep(1)
        documents = corpus.get_text_documents(0, 100)
        self.assertEqual(len(documents), 0)
        corpus.add_text_document("Life is gud when you are rich", "doc1", "english", 1)
        corpus.add_text_document("Life is bad when you are poor", "doc2", "english", 2)
        corpus.add_text_document("Another doc with auto id", "doc whaever", "english")
        corpus.add_text_document("Another doc whatever", "doc3", "english", 3)
        corpus.add_text_document("Another doc whatever++", "doc4", "english", 4)
        time.sleep(1)
        first2Docs = corpus.get_text_documents(0, 2)
        last2Docs = corpus.get_text_documents(2, 2)
        self.assertEqual(len(first2Docs), 2)
        self.assertEqual(len(last2Docs), 2)
        first2DocsIds = [first2Docs[0]["id"], first2Docs[1]["id"]]
        last2DocsIds = [last2Docs[0]["id"], last2Docs[1]["id"]]
        for id in first2DocsIds:
            if id in last2DocsIds:
                self.assertTrue(False, "Id should not overlap between doc sets")
                # should be 4 different IDS

    def test_add_document_duplicate_id_exception(self):
        corpus = self.documentCorpusList.create_corpus()
        corpus.add_text_document("Text Doc 1", "Title Doc 1", "english", "1")
        time.sleep(0.2)
        self.assertRaises(DocumentAlreadyExistsException, corpus.add_text_document, "Text Doc 1", "Title Doc 1",
                          "english", "1")

    def test_create_sub_corpus(self):
        corpus = self.documentCorpusList.create_corpus()
        subCorpus1 = corpus.create_sub_corpus("Sub corpus 1")
        subCorpus2 = corpus.create_sub_corpus("Sub corpus 2")
        es_wait_ready()

        getSubCorpus1 = corpus.get_sub_corpus(subCorpus1.id)
        getSubCorpus2 = corpus.get_sub_corpus(subCorpus2.id)
        self.assertEqual(subCorpus1.id, getSubCorpus1.id)
        self.assertEqual(subCorpus2.id, getSubCorpus2.id)

    def test_delete_corpus(self):
        corpus = self.documentCorpusList.create_corpus(languages=["english"])
        corpus.add_text_document("Doc 1", "10", "english")
        corpus.add_text_document("Doc 2", "20", "english")

        corpus2 = self.documentCorpusList.create_corpus()
        corpus2.add_text_document("Doc 1", "10", "english")
        corpusId = corpus.id

        self.documentCorpusList.delete_corpus(corpusId)
        time.sleep(2)  # just to be sure es had time to make deletions.

        es = get_es_conn()
        aliases = es.indices.get_alias().keys()
        # Check if any indices exist with corpus 1 index

        self.assertFalse(any(corpusId in s for s in aliases))

    """
    # SUB CORPUS NOT IN API, so NOT TESTED
    def test_add_document_sub_corpus(self):
        corpus = self.documentCorpusList.create_corpus(languages=["english", "french"])
        subCorpus1 = corpus.create_sub_corpus("Sub corpus 1")
        subCorpus2 = corpus.create_sub_corpus("Sub corpus 2")
        es_wait_ready()
        corpus.add_text_document("Doc 1", "10", "english")
        corpus.add_text_document("Doc 2", "20", "french")
        corpus.add_document_to_sub_corpus("10", subCorpus1, "english")
        corpus.add_document_to_sub_corpus("10", subCorpus2, "english")
        corpus.add_document_to_sub_corpus("20", subCorpus2, "french")
        time.sleep(2)
        subCorpus1DocRefs = subCorpus1.get_all_document_ref()
        self.assertEqual(len(subCorpus1DocRefs), 1)
        self.assertEqual(subCorpus1DocRefs[0]["id"], "10")
        self.assertEqual(subCorpus1DocRefs[0]["type"], "default")
        subCorpus2DocRefs = subCorpus2.get_all_document_ref()
        self.assertEqual(len(subCorpus2DocRefs), 2)

    def test_get_all_sub_corpuses_for_corpus(self):
        corpus = self.documentCorpusList.create_corpus()
        subCorpus1 = corpus.create_sub_corpus("Sub corpus 1")
        subCorpus2 = corpus.create_sub_corpus("Sub corpus 2")
        subCorpus3 = corpus.create_sub_corpus("Sub corpus 3")
        time.sleep(2)
        corpus2 = self.documentCorpusList.create_corpus()
        time.sleep(2)
        subCorpus21 = corpus2.create_sub_corpus("Sub corpus 2.1")
        self.assertEqual(3, len(corpus.get_all_sub_corpuses()))
        self.assertEqual(1, len(corpus2.get_all_sub_corpuses()))

    def test_delete_sub_corpus(self):
        corpus = self.documentCorpusList.create_corpus()
        subCorpus1 = corpus.create_sub_corpus("Sub corpus 1")
        subCorpus2 = corpus.create_sub_corpus("Sub corpus 2")
        subCorpus3 = corpus.create_sub_corpus("Sub corpus 3")
        corpus2 = self.documentCorpusList.create_corpus()
        subCorpus21 = corpus2.create_sub_corpus("Sub corpus 2.1")
        time.sleep(2)
        self.assertEqual(3, len(corpus.get_all_sub_corpuses()))
        self.assertEqual(1, len(corpus2.get_all_sub_corpuses()))
        corpus.delete_sub_corpus(subCorpus1.id)
        self.assertEqual(2, len(corpus.get_all_sub_corpuses()))
        self.assertEqual(1, len(corpus2.get_all_sub_corpuses()))
        corpus2.delete_sub_corpus(subCorpus21.id)
        self.assertEqual(2, len(corpus.get_all_sub_corpuses()))
        self.assertEqual(0, len(corpus2.get_all_sub_corpuses()))
        self.assertRaises(SubCorpusNotFoundException, corpus2.delete_sub_corpus, subCorpus21.id)

    """

    # buckets
    def test_create_bucket(self):
        corpus = self.documentCorpusList.create_corpus()
        bucket1 = corpus.create_bucket("bucket1")
        time.sleep(2)
        buckets = corpus.get_buckets()
        self.assertEqual(bucket1.id, buckets[0].id)

    def test_create_bucket_separated_corpus_id(self):
        #  Querying with '1-2' din't find buckets for corpusid '1-2' before because of analyser associations
        #  Specifying analysers for indexing: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-term-query.html

        corpus = self.documentCorpusList.create_corpus(id="1-2")
        bucket1 = corpus.create_bucket("bucket1")
        time.sleep(1)
        buckets = corpus.get_buckets()
        self.assertEqual(bucket1.id, buckets[0].id)

    def test_create_bucket_simple_corpus_id(self):
        corpus = self.documentCorpusList.create_corpus(id="1")
        bucket1 = corpus.create_bucket("bucket1")
        time.sleep(1)
        buckets = corpus.get_buckets()
        self.assertEqual(bucket1.id, buckets[0].id)

    def test_create_bucket_simple_bucket_id(self):
        corpus = self.documentCorpusList.create_corpus()
        bucket1 = corpus.create_bucket("bucket1", bucketId="1")
        time.sleep(1)
        buckets = corpus.get_buckets()
        self.assertEqual(bucket1.id, buckets[0].id)

    def test_create_bucket_simple_ids(self):
        corpus = self.documentCorpusList.create_corpus(id="1")
        bucket1 = corpus.create_bucket("bucket1", bucketId="1")
        time.sleep(1)
        buckets = corpus.get_buckets()
        self.assertEqual(bucket1.id, buckets[0].id)

    def test_create_bucket_generated_id_not_dependant_on_name(self):
        corpus = self.documentCorpusList.create_corpus()
        bucket_name = "bucket1"
        bucket1 = corpus.create_bucket(bucket_name)
        self.assertNotIn(bucket_name.lower(), bucket1.id.lower(),
                         "Generated bucket id must not be based on bucket name")

    def test_create_bucket_duplicate_names(self):
        corpus = self.documentCorpusList.create_corpus()
        bucket1 = corpus.create_bucket("bucket1")
        time.sleep(1)
        bucket1 = corpus.create_bucket("bucket1")
        time.sleep(1)
        buckets = corpus.get_buckets()
        self.assertEqual(len(buckets), 2)

    def test_delete_bucket(self):
        corpus = self.documentCorpusList.create_corpus()
        bucket1 = corpus.create_bucket("bucket1")
        bucket2 = corpus.create_bucket("bucket2")
        time.sleep(1)
        self.assertEqual(len(corpus.get_buckets()), 2)
        self.bucketList.delete_bucket(corpus.id, bucket1.id)
        time.sleep(1)
        self.assertEqual(len(corpus.get_buckets()), 1)

    def test_add_annotation(self):
        corpus = self.documentCorpusList.create_corpus()
        bucket1 = corpus.create_bucket("bucket1")
        anno1 = {
            "id": "id1",
            "docId": "1",
            "myfield": "f1",
            "category": "A"
        }
        anno2 = {
            "docId": "2",
            "myfield": "f2",
            "category": "A"
        }
        anno3 = {
            "docId": "2",
            "myfield": "f2",
            "category": "B"
        }

        bucket1.add_annotation(anno1, "type1", "id1")
        bucket1.add_annotation(anno2, "type1")
        bucket1.add_annotation(anno3, "type1")
        time.sleep(0.1)

    def test_get_annotation_uid(self):
        corpus = self.documentCorpusList.create_corpus()
        bucket1 = corpus.create_bucket("bucketname")
        anno1 = {
            "myfield": "f1",
            "category": "A"
        }
        docType = "type1"
        annoId = bucket1.add_annotation(anno1, docType)
        time.sleep(0.1)
        readAnno = bucket1.get_annotation(annoId, docType)
        time.sleep(0.1)

    def test_get_annotation_simple_id(self):
        corpus = self.documentCorpusList.create_corpus("c1")
        bucket1 = corpus.create_bucket("bucketname", bucketId="b1")
        anno1 = {
            "docId": "1",
            "myfield": "f1",
            "category": "A"
        }
        docType = "type1"
        annoId = bucket1.add_annotation(anno1, docType, annotationId="a1")
        time.sleep(0.1)
        readAnno = bucket1.get_annotation(annoId, docType)
        time.sleep(0.1)


    def test_update_annotation(self):
        corpus = self.documentCorpusList.create_corpus()
        bucket1 = corpus.create_bucket("bucket1")
        anno1 = {
            "docId": "1",
            "myfield": "f1",
            "category": "A"
        }
        anno1up = {
            "docId": "1",
            "myfield": "f1",
            "category": "B"
        }
        bucket1.add_annotation(anno1, "type1", "id1")
        time.sleep(0.1)
        bucket1.update_annotation(anno1up, "type1", "id1")
        time.sleep(0.1)
        annores = bucket1.get_annotation("id1", "type1")
        self.assertEqual(annores["category"], "B")

    def test_delete_annotation(self):
        corpus = self.documentCorpusList.create_corpus()
        bucket1 = corpus.create_bucket("bucket1")
        anno1 = {
            "id": "id1",
            "docId": "1",
            "myfield": "f1",
            "category": "A"
        }
        anno2 = {
            "docId": "2",
            "myfield": "f2",
            "category": "A"
        }
        anno3 = {
            "docId": "2",
            "myfield": "f2",
            "category": "B"
        }

        bucket1.add_annotation(anno1, "type1", "id1")
        time.sleep(0.1)
        bucket1.add_annotation(anno2, "type1")
        time.sleep(0.1)
        bucket1.delete_annotation("id1", "type1")
        time.sleep(0.1)
        self.assertRaises(DocumentNotFoundException, bucket1.get_annotation, "id1", "type1")

    def test_small_search_test_bucket_isolation(self):
        # All annotations have the same ID for each bucket. This is to make sure data and search are well isolated.
        c1b1a1 = {
            "docId": "1",
            "path": "c1b1a1",
            "category": "A"
        }
        c1b1a2 = {
            "docId": "1",
            "path": "c1b1a2",
            "category": "B"
        }
        c1b2a3 = {
            "docId": "1",
            "path": "c1b2a1",
            "category": "C"
        }
        c1b2a4 = {
            "docId": "1",
            "path": "c1b2a1",
            "category": "D"
        }
        c2b3a5 = {
            "docId": "10",
            "path": "c2b1a1",
            "category": "E"
        }

        c1 = self.documentCorpusList.create_corpus()
        c2 = self.documentCorpusList.create_corpus()
        bucketList = get_master_bucket_list(self.envId, self.authorization)
        b1 = bucketList.create_bucket("b1", c1.id)
        b2 = bucketList.create_bucket("b2", c1.id)
        b3 = bucketList.create_bucket("b3", c2.id)
        time.sleep(0.1)
        b1.add_annotation(c1b1a1, "t1")
        b1.add_annotation(c1b1a2, "t1")
        b2.add_annotation(c1b2a3, "t2")
        b2.add_annotation(c1b2a4, "t2")
        b3.add_annotation(c2b3a5, "t2")

    def tearDown(self):
        es = get_es_conn()
        es_wait_ready()
        es.indices.delete(index="unittest_*")
        time.sleep(0.1)

    @classmethod
    def tearDownClass(cls):
        es = get_es_conn()
        es_wait_ready()
        es.indices.delete(index="unittest_*")
        es_wait_ready()


if __name__ == '__main__':
    unittest.main()
