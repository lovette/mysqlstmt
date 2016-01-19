# -*- coding: utf-8 -*-

import unittest
from nose.tools import assert_equals, raises
from mysqlstmt import Union, Select


class TestUnion(unittest.TestCase):
    def test_single(self):
        q = Union()
        sql_t = q.union('SELECT `t1c1` FROM t1').sql()
        assert_equals(sql_t, ('(SELECT `t1c1` FROM t1)', None))

    def test_double(self):
        q = Union()
        sql_t = q.union('SELECT `t1c1` FROM t1').select('SELECT `t2c1` FROM t2').sql()
        assert_equals(sql_t, ('(SELECT `t1c1` FROM t1) UNION (SELECT `t2c1` FROM t2)', None))

    def test_orderby(self):
        q = Union()
        sql_t = q.union('SELECT `t1c1` AS sort_col FROM t1').select('SELECT `t2c1` FROM t2').order_by('sort_col, DESC').sql()
        assert_equals(sql_t, ('(SELECT `t1c1` AS sort_col FROM t1) UNION (SELECT `t2c1` FROM t2) ORDER BY sort_col, DESC', None))

    def test_orderby_limit(self):
        q = Union()
        sql_t = q.union('SELECT `t1c1` AS sort_col FROM t1').select('SELECT `t2c1` FROM t2').order_by('sort_col, DESC').limit(5).sql()
        assert_equals(sql_t, ('(SELECT `t1c1` AS sort_col FROM t1) UNION (SELECT `t2c1` FROM t2) ORDER BY sort_col, DESC LIMIT 5', None))

    def test_orderby_limit_offset(self):
        q = Union()
        sql_t = q.union('SELECT `t1c1` AS sort_col FROM t1').select('SELECT `t2c1` FROM t2').order_by('sort_col, DESC').limit(10, 5).sql()
        assert_equals(sql_t, ('(SELECT `t1c1` AS sort_col FROM t1) UNION (SELECT `t2c1` FROM t2) ORDER BY sort_col, DESC LIMIT 5,10', None))

    def test_single_obj(self):
        s = Select('t1').column('t1c1')
        q = Union()
        sql_t = q.union(s).sql()
        assert_equals(sql_t, ('(SELECT `t1c1` FROM t1)', None))

    def test_double_obj(self):
        s1 = Select('t1').column('t1c1')
        s2 = Select('t2').column('t2c1')
        q = Union()
        sql_t = q.union(s1).union(s2).sql()
        assert_equals(sql_t, ('(SELECT `t1c1` FROM t1) UNION (SELECT `t2c1` FROM t2)', None))

    def test_double_obj_list(self):
        s1 = Select('t1').column('t1c1')
        s2 = Select('t2').column('t2c1')
        q = Union()
        sql_t = q.union([s1, s2]).sql()
        assert_equals(sql_t, ('(SELECT `t1c1` FROM t1) UNION (SELECT `t2c1` FROM t2)', None))

    def test_double_obj_list_constructor(self):
        s1 = Select('t1').column('t1c1')
        s2 = Select('t2').column('t2c1')
        q = Union([s1, s2])
        sql_t = q.sql()
        assert_equals(sql_t, ('(SELECT `t1c1` FROM t1) UNION (SELECT `t2c1` FROM t2)', None))

    def test_union_all(self):
        q = Union(distinct=False)
        sql_t = q.union('SELECT `t1c1` FROM t1').select('SELECT `t2c1` FROM t2').sql()
        assert_equals(sql_t, ('(SELECT `t1c1` FROM t1) UNION ALL (SELECT `t2c1` FROM t2)', None))

    @raises(ValueError)
    def test_fail_no_stmts(self):
        q = Union()
        q.sql()
