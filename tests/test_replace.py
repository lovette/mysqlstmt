import unittest

from nose.tools import assert_equals

from mysqlstmt import Replace


class TestReplace(unittest.TestCase):
    def test_set_value_int(self):
        q = Replace()
        sql_t = q.into_table('t').set_value('c1', 1).sql()
        assert_equals(sql_t, ('REPLACE INTO t (`c1`) VALUES (1)', None))
