# coding: utf-8

__author__ = 'zakharan'

import unittest

from jassrealtime.core.settings_utils import *


class MyTestCase(unittest.TestCase):
    def test_get_settings_default_location(self):
        set = get_settings()
        self.assertEqual(set['SERVER_NAME'], "localhost")


if __name__ == '__main__':
    unittest.main()
