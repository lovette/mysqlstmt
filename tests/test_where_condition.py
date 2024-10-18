import unittest
from nose.tools import assert_equals
from mysqlstmt import Select, WhereCondition


class TestWhereCondition(unittest.TestCase):
    def test_empty(self):
        c = WhereCondition(Select())
        params = []
        sql = c.sql(params)
        assert_equals(sql, None)

    def test_nesting(self):
        c1 = WhereCondition(Select())
        c2 = WhereCondition(Select())
        c3 = WhereCondition(Select())
        c1.add_cond(c2)
        c2.add_cond(c3)
        assert_equals(c1.nesting_level, 0)
        assert_equals(c2.nesting_level, 1)
        assert_equals(c3.nesting_level, 2)

    def test_nesting2(self):
        c1 = WhereCondition(Select())
        c2 = WhereCondition(Select())
        c3 = WhereCondition(Select())
        c2.add_cond(c3)
        c1.add_cond(c2)
        assert_equals(c1.nesting_level, 0)
        assert_equals(c2.nesting_level, 1)
        assert_equals(c3.nesting_level, 2)

    def test_where_nesting_empty(self):
        c1 = WhereCondition(Select())
        c2 = WhereCondition(Select())
        c1.add_cond(c2)
        params = []
        sql = c1.sql(params)
        assert_equals(sql, None)

    def test_where_cond(self):
        c = WhereCondition(Select())
        params = []
        sql = c.where_value('t1c1', 3).sql(params)
        assert_equals(sql, '`t1c1` = 3')

    def test_where_conds(self):
        c1 = WhereCondition(Select())
        c2 = WhereCondition(Select())
        c1.add_cond(c2)
        params = []
        sql = c1.where_value('t1c1', 3).sql(params)
        assert_equals(sql, '`t1c1` = 3')
