import unittest
from jassrealtime.core.env import *
from jassrealtime.core.settings_utils import *
from jassrealtime.core.master_factory_list import get_env_list, get_master_document_corpus_list, \
    get_master_bucket_list
from jassrealtime.document.bucket import *
import asyncio
import concurrent.futures
import requests
from random import random, shuffle
import time

"""
This is an integration test which test the interaction between all components of JASSTealTime.
This integration assumes app_test.py server is running on port 8889
"""


class MyTestCase(unittest.TestCase):
    """
    TODO: NOT WORKING NEED FIX
    def test_create_env_corpus_document_bucket_annotation(self):
        try:
            setting = get_settings()
            self.envId = "unittest2_"
            self.authorization = BaseAuthorization("unittest2_", None, None, None)
            self.envList = get_env_list(self.authorization)

            try:
                self.envList.create_env(self.envId)
            except EnvAlreadyExistWithSameIdException:
                time.sleep(1)
                self.envList.delete_env(self.envId)
                time.sleep(1)
                self.envList.create_env(self.envId)
        finally:
            pass

        # testing corpus,schema,annotation to make sure they work.
        c1b1a1 = {
            "docId": "1",
            "path": "c1b1a1",
            "category": "A"
        }

        documentCorpusList = get_master_document_corpus_list(self.envId, self.authorization)
        bucketList = get_master_bucket_list(self.envId, self.authorization)

        c1 = documentCorpusList.create_corpus("corpus1")
        c1.add_text_document("Doc 2", 1, "english")
        b1 = bucketList.create_bucket("b1", c1.id, "docId", "b1", TargetType.document)
        time.sleep(0.1)
        b1.add_annotation(c1b1a1, "t1")
        time.sleep(1)
    """

    async def _test_longrunningrequest_async(self):
        start = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=get_nb_cores()) as executor:
            loop = asyncio.get_event_loop()
            futures = [loop.run_in_executor(
                executor,
                requests.get,
                'http://localhost:8889/test/longrunningrequest/{0}'.format(random())
            ) for i in range(get_nb_cores() - 1)]
            for response in await asyncio.gather(*futures):
                times = response.content.decode("utf-8").split(',')
                self.assertTrue(float(times[3]) < 8,
                                "Time to execute {0} should be less then 8 seconds".format(times[3]))
                timeSinceStart = float(times[2]) - start
                self.assertTrue(timeSinceStart < 10,
                                "Time to execute {0} should be less then 10 seconds from start of processes".format(
                                    timeSinceStart))

    async def _test_mix_running_request_long_first_async(self):
        start = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=get_nb_cores()) as executor:
            loop = asyncio.get_event_loop()
            # 1 less long requests then number of cores
            # generate requests and

            futures = [loop.run_in_executor(
                executor,
                requests.get,
                'http://localhost:8889/test/longrunningrequest/{0}'.format(random())
            ) for i in range(get_nb_cores() - 2)]
            futures = [loop.run_in_executor(
                executor,
                requests.get,
                'http://localhost:8889/test/shortrunningrequest/{0}'.format(random())
            ) for i in range(99)]

            for response in await asyncio.gather(*futures):
                times = response.content.decode("utf-8").split(',')
                if times[0] == "long":
                    self.assertTrue(float(times[3]) < 8,
                                    "Time to execute {0} should be less then 8 seconds".format(times[3]))
                    timeSinceStart = float(times[2]) - start
                    self.assertTrue(timeSinceStart < 10,
                                    "Time to execute {0} should be less then 10 seconds from start of processes".format(
                                        timeSinceStart))
                else:
                    self.assertTrue(float(times[3]) < 1,
                                    "Time to execute {0} should be less then 1 seconds".format(times[3]))
                timeSinceStart = float(times[2]) - start
                self.assertTrue(timeSinceStart < 1,
                                "Time to execute {0} should be less then 1 seconds from start of processes".format(
                                    timeSinceStart))

    async def _test_mix_running_request_shuffled_async(self):
        start = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=get_nb_cores()) as executor:
            loop = asyncio.get_event_loop()
            # 1 less long requests then number of cores
            # generate requests and
            requestUrls = []
            for i in range(get_nb_cores() - 2):
                requestUrls.append('http://localhost:8889/test/longrunningrequest/{0}'.format(random()))

            for i in range(99):
                requestUrls.append('http://localhost:8889/test/shortrunningrequest/{0}'.format(random()))

            shuffle(requestUrls)

            futures = [loop.run_in_executor(
                executor,
                requests.get,
                request
            ) for request in requestUrls]

            for response in await asyncio.gather(*futures):
                times = response.content.decode("utf-8").split(',')
                if times[0] == "long":
                    self.assertTrue(float(times[3]) < 8,
                                    "Time to execute {0} should be less then 8 seconds".format(times[3]))
                    timeSinceStart = float(times[2]) - start
                    self.assertTrue(timeSinceStart < 10,
                                    "Time to execute {0} should be less then 10 seconds from start of processes".format(
                                        timeSinceStart))
                else:
                    self.assertTrue(float(times[3]) < 1,
                                    "Time to execute {0} should be less then 1 seconds".format(times[3]))
                timeSinceStart = float(times[2]) - start
                self.assertTrue(timeSinceStart < 1,
                                "Time to execute {0} should be less then 1 seconds from start of processes".format(
                                    timeSinceStart))

    """
    async def _test_mix_running_sub_process_request_shuffled_async(self):
        start = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=get_nb_cores()) as executor:
            loop = asyncio.get_event_loop()
            # 1 less long requests then number of cores
            # generate requests and
            requestUrls = []
            for i in range(get_nb_cores() - 2):
                requestUrls.append('http://localhost:8889/test/longsubrunningrequest/{0}'.format(random()))

            #for i in range(99):
            #    requestUrls.append('http://localhost:8889/test/shortrunningrequest/{0}'.format(random()))

            shuffle(requestUrls)

            futures = [loop.run_in_executor(
                executor,
                requests.get,
                request
            ) for request in requestUrls]

            for response in await asyncio.gather(*futures):
                times = response.content.decode("utf-8").split(',')
                try:
                    if times[0] == "long":
                        self.assertTrue(float(times[3]) < 8,
                                        "Time to execute {0} should be less then 8 seconds".format(times[3]))
                        timeSinceStart = float(times[2]) - start
                        self.assertTrue(timeSinceStart < 10,
                                        "Time to execute {0} should be less then 10 seconds from start of processes".format(
                                            timeSinceStart))
                    else:
                        self.assertTrue(float(times[3]) < 1,
                                        "Time to execute {0} should be less then 1 seconds".format(times[3]))
                    timeSinceStart = float(times[2]) - start
                    self.assertTrue(timeSinceStart < 1,
                                    "Time to execute {0} should be less then 1 seconds from start of processes".format(
                                        timeSinceStart))
                except Exception as e:
                    print(e)
    """

    def test_long_requests_concurrency(self):
        """
        This tests assumes a modern machine with 4 cores. It will create 1 worker per core.
        :return:
        """
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._test_longrunningrequest_async())

    def test_mix_running_request_long_first_async(self):
        """
        This tests assumes a modern machine with 4 cores. It will create 1 worker per core.
        :return:
        """
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._test_mix_running_request_long_first_async())

    def test_mix_running_request_shuffled_async(self):
        """
        This tests assumes a modern machine with 4 cores. It will create 1 worker per core.
        :return:
        """
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._test_mix_running_request_shuffled_async())

    """
    Does NOT WORK, since parent call hangs
    def test_mix_running_sub_process_request_shuffled_async(self):

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._test_mix_running_sub_process_request_shuffled_async())
    """

    @classmethod
    def tearDownClass(cls):
        es = get_es_conn()
        try:
            es_wait_ready()
            es.indices.delete(index="unittest_*")
        except:
            pass
