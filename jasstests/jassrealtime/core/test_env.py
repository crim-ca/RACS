import unittest
from jassrealtime.core.env import *
from jassrealtime.core.settings_utils import *
from jassrealtime.core.master_factory_list import get_env_list


class MyTestCase(unittest.TestCase):
    def test_create_env(self):
        es = get_es_conn()
        es_wait_ready()
        sett = get_settings()
        authorization1 = BaseAuthorization("unitenvtest", None, None, None)
        envList1 = get_env_list(authorization1)

        authorization2 = BaseAuthorization("unitenvtest2", None, None, None)
        envList2 = get_env_list(authorization2)

        envList1.create_env("unitenvtest")
        envList1.create_env("unitenvtest2", "My Super env")
        envList2.delete_env("unitenvtest")
        envList2.delete_env("unitenvtest2")
        time.sleep(1)
        envList1.create_env("unitenvtest")
        envList2.delete_env("unitenvtest")
        # testing corpus,schema,annotation to make sure they work.


if __name__ == '__main__':
    unittest.main()
