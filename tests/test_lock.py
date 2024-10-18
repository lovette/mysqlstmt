import unittest
from nose.tools import assert_equals, raises
from mysqlstmt import Lock


class TestLock(unittest.TestCase):
    def test_unnamed_params(self):
        q = Lock('mylock', 5)
        sql_t = q.sql()
        assert_equals(sql_t, ("SELECT GET_LOCK('mylock', 5)", None))

    def test_named_params(self):
        q = Lock(name='mylock', timeout=5)
        sql_t = q.sql()
        assert_equals(sql_t, ("SELECT GET_LOCK('mylock', 5)", None))

    def test_release(self):
        q = Lock('mylock', 5)
        sql_t = q.release_lock()
        assert_equals(sql_t, ("SELECT RELEASE_LOCK('mylock')", None))

    def test_free(self):
        q = Lock('mylock', 5)
        sql_t = q.is_free_lock()
        assert_equals(sql_t, ("SELECT IS_FREE_LOCK('mylock')", None))

    @raises(ValueError)
    def test_fail_no_name(self):
        q = Lock()
        q.sql()

    @raises(ValueError)
    def test_fail_no_timeout(self):
        q = Lock('mylock')
        q.sql()
