import unittest
import glob, os, shutil, zipfile
import requests, json
from jassrealtime.document.bucket import *
from jassrealtime.core.master_factory_list import get_env_list, get_master_document_corpus_list
from jassrealtime.core.env import EnvAlreadyExistWithSameIdException
from jassrealtime.batch.document_corpus import DocumentCorpus

ENCODING = "utf-8"

CORPUS_ID = "corpus1"

JASS_TEST_DATA_PATH = os.path.join("/tmp", "jass_test_data")
POST_URL = 'http://localhost:25478/upload?token=f9403fc5f537b4ab332d'
GET_URL = 'http://localhost:25478/files/3docs.zip?token=f9403fc5f537b4ab332d'


class MyTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # make a tmp dir and copy all test data files into it.
        if os.path.exists(JASS_TEST_DATA_PATH):
            shutil.rmtree(JASS_TEST_DATA_PATH)
        os.makedirs(JASS_TEST_DATA_PATH)
        scriptPath = os.path.abspath(__file__)
        scriptDataDir = os.path.join(os.path.dirname(scriptPath), "jass_document_corpus_data")
        files = glob.iglob(os.path.join(scriptDataDir, "*.txt"))
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

    def set_up_corpus(self):
        corpus = get_master_document_corpus_list(self.envId, self.authorization).create_corpus(CORPUS_ID,
                                                                                               languages=["en-US"])
        files = glob.iglob(os.path.join(JASS_TEST_DATA_PATH, "*.txt"))
        self.contentById = dict()
        for filePath in files:
            with open(filePath, 'r', encoding="utf8") as f:
                id = os.path.basename(filePath)
                contents = f.read()
                self.contentById[str(id) + ".txt"] = contents
                corpus.add_text_document(contents, filePath, "en-US", id)
        time.sleep(1)

        # def test_upload_documents(self):
        #    # note you need to manually run test server for this test to pass
        #    # Use: docker run -p 25478:25478 -v /tmp/http_post_file_storage:/var/root mayth/simple-upload-server app -token f9403fc5f537b4ab332d /var/root
        #    self.set_up_corpus()
        #    documentCorpus = DocumentCorpus(self.envId,self.authorization,"corpus1")
        #    documentCorpus.upload_documents(POST_URL,"3docs.zip")
        #    response = requests.get(GET_URL, stream=True)
        #    outFilePath = os.path.join(JASS_TEST_DATA_PATH,'output.zip')
        #    with open(outFilePath,'wb') as f:
        #        for block in response.iter_content(1024):
        #            f.write(block)

    #
    #    zipHandle = zipfile.ZipFile(outFilePath, 'r')
    #    zipHandle.extractall(JASS_TEST_DATA_PATH)
    #    zipHandle.close()
    #
    #    for fileName,data in self.contentById.items():
    #        with open(os.path.join(JASS_TEST_DATA_PATH,fileName),'r',encoding='utf8') as f:
    #            self.assertEqual(data,f.read())

    @unittest.skip("This is more of an integration test with an external file server")
    def test_upload_documents_mss(self):
        # INTEGRATION TEST with leads mss
        self.set_up_corpus()
        uploadFileName = "jass_mss_documents_test.zip"
        resp = requests.get("http://services-leads.vesta.crim.ca/multimedia_storage/v1_7/add",
                            params={"filename": uploadFileName})
        swift_url_info = json.loads(resp.text)
        documentCorpus = DocumentCorpus(self.envId, self.authorization, CORPUS_ID)
        documentCorpus.upload_documents(swift_url_info["upload_url"], uploadFileName, True, False)
        time.sleep(2)
        # getting the file we uploaded
        response = requests.get("http://services-leads.vesta.crim.ca/multimedia_storage/v1_7/get/"
                                + swift_url_info["storage_doc_id"], stream=True)

        outFilePath = os.path.join(JASS_TEST_DATA_PATH, 'output2.zip')
        with open(outFilePath, 'wb') as f:
            for block in response.iter_content(1024):
                f.write(block)

        zipHandle = zipfile.ZipFile(outFilePath, 'r')
        zipHandle.extractall(JASS_TEST_DATA_PATH)
        zipHandle.close()

        for fileName, data in self.contentById.items():
            with open(os.path.join(JASS_TEST_DATA_PATH, fileName), 'r', encoding='utf8') as f:
                self.assertEqual(data, f.read())

    def test_get_documents(self):
        """
        Verify that each file added in the corpus is present in the zip file and is the same file.
        We compare files with the encoding applied when adding the documents to a corpus.
        """
        self.set_up_corpus()
        document_corpus = DocumentCorpus(self.envId, self.authorization, CORPUS_ID)
        zip_path = document_corpus.get_documents_zip()
        with zipfile.ZipFile(zip_path) as documents_zip:
            files = glob.iglob(os.path.join(JASS_TEST_DATA_PATH, "*.txt"))
            original_files_count = 0
            for filePath in files:
                with open(filePath, 'r', encoding=ENCODING) as document:
                    original_files_count += 1
                    text_id = os.path.basename(filePath)
                    original_contents = document.read()
                    with documents_zip.open(str(text_id) + ".txt") as document_zip:
                        retrieved_content = document_zip.read().decode(ENCODING)
                        self.assertEqual(original_contents, retrieved_content)
            self.assertEqual(len(documents_zip.infolist()), original_files_count)

        document_corpus.clear_temporary_files()

