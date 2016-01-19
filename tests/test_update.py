# -*- coding: utf-8 -*-

import unittest
from nose.tools import assert_equals, raises
from mysqlstmt import Update
from collections import OrderedDict

class TestUpdate(unittest.TestCase):
    def test_constructor_table_name(self):
        q = Update('t1')
        sql_t = q.set_value('t1c1', 1).sql()
        assert_equals(sql_t, ('UPDATE t1 SET `t1c1`=1', None))

    def test_set_value_int(self):
        q = Update()
        sql_t = q.table('t1').set_value('t1c1', 1).sql()
        assert_equals(sql_t, ('UPDATE t1 SET `t1c1`=1', None))

    def test_set_value_int_callable(self):
        q = Update()
        sql_t = q.table('t1').set_value('t1c1', 1)()
        assert_equals(sql_t, ('UPDATE t1 SET `t1c1`=1', None))

    def test_set_value_ints(self):
        q = Update()
        sql_t = q.table('t1').set_value('t1c1', 1).set_value('t1c2', 2).sql()
        assert_equals(sql_t, ('UPDATE t1 SET `t1c1`=1, `t1c2`=2', None))

    def test_dict_int(self):
        q = Update()
        values = {'t1c1': 1}
        sql_t = q.table('t1').set_value(values).sql()
        assert_equals(sql_t, ('UPDATE t1 SET `t1c1`=1', None))

    def test_dict_ints(self):
        q = Update()
        values = OrderedDict([('t1c1',1), ('t1c2',2)])
        sql_t = q.table('t1').set_value(values).sql()
        assert_equals(sql_t, ('UPDATE t1 SET `t1c1`=1, `t1c2`=2', None))

    def test_dict_strings(self):
        q = Update()
        values = OrderedDict([('t1c1','a'), ('t1c2','b')])
        sql_t = q.table('t1').set_value(values).sql()
        assert_equals(sql_t, ("UPDATE t1 SET `t1c1`=?, `t1c2`=?", ['a','b']))

    def test_null(self):
        q = Update()
        values = OrderedDict([('t1c1','a'), ('t1c2',None)])
        sql_t = q.table('t1').set_value(values).sql()
        assert_equals(sql_t, ("UPDATE t1 SET `t1c1`=?, `t1c2`=NULL", ['a']))

    def test_function_value(self):
        q = Update()
        sql_t = q.table('t1').set_value('t1c1', 'NOW()').sql()
        assert_equals(sql_t, ('UPDATE t1 SET `t1c1`=?', ['NOW()']))

    def test_function_raw_value(self):
        q = Update()
        sql_t = q.table('t1').set_raw_value('t1c1', 't1c1+1').sql()
        assert_equals(sql_t, ('UPDATE t1 SET `t1c1`=t1c1+1', None))

    def test_function_raw_value_dict(self):
        q = Update()
        sql_t = q.table('t1').set_raw_value({'t1c1':'NOW()'}).sql()
        assert_equals(sql_t, ('UPDATE t1 SET `t1c1`=NOW()', None))

    def test_function_raw_value_with_valparams(self):
        q = Update()
        sql_t = q.table('t1').set_raw_value('t1c1', 'PASSWORD(?)', value_params=('mypw',)).sql()
        assert_equals(sql_t, ('UPDATE t1 SET `t1c1`=PASSWORD(?)', ['mypw']))

    def test_function_raw_value_dict_with_valparams(self):
        q = Update()
        sql_t = q.table('t1').set_raw_value({'t1c1':('PASSWORD(?)', ('mypw',))}).sql()
        assert_equals(sql_t, ('UPDATE t1 SET `t1c1`=PASSWORD(?)', ['mypw']))

    def test_order_by(self):
        q = Update()
        sql_t = q.table('t1').set_value('t1c1', 1).order_by('t1c2').sql()
        assert_equals(sql_t, ('UPDATE t1 SET `t1c1`=1 ORDER BY t1c2', None))

    def test_order_bys(self):
        q = Update()
        sql_t = q.table('t1').set_value('t1c1', 1).order_by(['t1c1','t1c2']).sql()
        assert_equals(sql_t, ('UPDATE t1 SET `t1c1`=1 ORDER BY t1c1, t1c2', None))

    def test_limit(self):
        q = Update()
        sql_t = q.table('t1').set_value('t1c1', 1).limit(5).sql()
        assert_equals(sql_t, ('UPDATE t1 SET `t1c1`=1 LIMIT 5', None))

    def test_ignore(self):
        q = Update(ignore_error=True)
        sql_t = q.table('t1').set_value('t1c1', 1).sql()
        assert_equals(sql_t, ('UPDATE IGNORE t1 SET `t1c1`=1', None))

    def test_where(self):
        q = Update()
        sql_t = q.table('t1').set_value('t1c1', 1).where_value('t1c2', 5).sql()
        assert_equals(sql_t, ('UPDATE t1 SET `t1c1`=1 WHERE `t1c2` = 5', None))

    def test_join(self):
        q = Update()
        sql_t = q.table('t1').set_value('t1c1', 1).join('t2', 't1c1').where_value('t2c1', None).sql()
        assert_equals(sql_t, ('UPDATE t1 INNER JOIN t2 USING (`t1c1`) SET `t1c1`=1 WHERE `t2c1` IS NULL', None))

    def test_join_2(self):
        q = Update()
        sql_t = q.table(['t1','t2']).set_value('t1c1', 1).where_expr('(`t1c1` = `t2c1`)').sql()
        assert_equals(sql_t, ('UPDATE t1, t2 SET `t1c1`=1 WHERE (`t1c1` = `t2c1`)', None))

    def test_noparam(self):
        q = Update(placeholder=False)
        sql = q.table('t1').set_value('t1c1', 1).where_value('t1c2', 5).sql()
        assert_equals(sql, 'UPDATE t1 SET `t1c1`=1 WHERE `t1c2` = 5')

    def test_set_value_utf_param(self):
        q = Update()
        sql_t = q.table('t1').set_value('t1c1', u'äöü').sql()
        assert_equals(sql_t, ('UPDATE t1 SET `t1c1`=?', [u'äöü']))

    def test_set_value_raw_utf(self):
        q = Update()
        sql_t = q.table('t1').set_raw_value('t1c1', u'"äöü"').sql()
        assert_equals(sql_t, (u'UPDATE t1 SET `t1c1`="äöü"', None))

    def test_set_value_utf_noparam(self):
        q = Update(placeholder=False)
        sql = q.table('t1').set_value('t1c1', u'"äöü"').sql()
        assert_equals(sql, u'UPDATE t1 SET `t1c1`="äöü"', None)

    def test_set_value_int_options(self):
        q = Update()
        sql_t = q.set_option(['LOW_PRIORITY']).table('t1').set_value('t1c1', 1).sql()
        assert_equals(sql_t, ('UPDATE LOW_PRIORITY t1 SET `t1c1`=1', None))

    @raises(ValueError)
    def test_fail_no_table(self):
        q = Update()
        q.set_value('t1c1', 1).sql()

    @raises(ValueError)
    def test_fail_no_value(self):
        q = Update()
        q.table('t1').sql()

    @raises(ValueError)
    def test_fail_order_by(self):
        q = Update()
        q.table('t1').set_value('t1c1', 1).join('t2', 't1c1').where_value('t2c1', None).order_by('t1c1').sql()

    @raises(ValueError)
    def test_fail_limit(self):
        q = Update()
        q.table('t1').set_value('t1c1', 1).join('t2', 't1c1').where_value('t2c1', None).limit(5).sql()
