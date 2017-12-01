import unittest
from jassrealtime.core.esutils import *


class MyTestCase(unittest.TestCase):
    def test_check_indices_name_valid_for_delete(self):
        self.assertTrue(check_indices_name_valid_for_delete("env_class_*", "env_", "class_"))
        self.assertTrue(check_indices_name_valid_for_delete("env_CLASS_A", "env_", "CLASS_"))
        self.assertTrue(check_indices_name_valid_for_delete("env_class_index1_-a", "env_", "class_"))
        self.assertTrue(check_indices_name_valid_for_delete("env_class_", "env_", "class_"))

        self.assertTrue(
            check_indices_name_valid_for_delete("env_class_index1,env_class_index2", "env_", "class_"))
        self.assertFalse(
            check_indices_name_valid_for_delete("env_class_index%$,env_class_index2", "env_", "class_"))
        self.assertFalse(
            check_indices_name_valid_for_delete("envX_class_index,env_class_index2", "env_", "class_"))
        self.assertFalse(
            check_indices_name_valid_for_delete("env_classY_index,env_class_index2", "env_", "class_"))
        self.assertFalse(
            check_indices_name_valid_for_delete("env_class_index,env_class_index2*", "env_", "class_"))
        self.assertFalse(check_indices_name_valid_for_delete("env_class_a,", "env_", "class_"))
        self.assertFalse(check_indices_name_valid_for_delete("*env_class_a", "env_", "class_"))

    def test_check_index_name_valid_for_create(self):
        self.assertTrue(check_index_name_valid_for_create("env_CLASS_A", "env_", "CLASS_"))
        self.assertTrue(check_index_name_valid_for_create("env_class_index1_-a", "env_", "class_"))
        self.assertFalse(check_index_name_valid_for_create("env_class_*", "env_", "class_"))
        self.assertFalse(check_index_name_valid_for_create("env_class_", "env_", "class_"))
        self.assertFalse(check_index_name_valid_for_create("env_class_a,", "env_", "class_"))
        self.assertFalse(check_index_name_valid_for_create("*env_class_a", "env_", "class_"))


if __name__ == '__main__':
    unittest.main()
