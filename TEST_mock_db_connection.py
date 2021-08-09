import unittest
from unittest.mock import MagicMock
from run_query import *


# Class to mock DB connection
class TestDb(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print("Starting test to mock db connection.")

    @classmethod
    def tearDownClass(cls):
        print("Tearing down the TestDb class")

    # to test on mocking the db connection
    def test_read(self):
        expected = [(3, 'John')]

        mock_connect = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = expected
        mock_connect.cursor.return_value = mock_cursor
        result = read_query("select * from sample", mock_connect, "testing")
        self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()
