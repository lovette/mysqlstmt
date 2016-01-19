# -*- coding: utf-8 -*-

import unittest
from nose.tools import assert_equals, raises
from mysqlstmt import Select
from collections import OrderedDict
import datetime

class TestSelect(unittest.TestCase):
    def test_constructor_table_name(self):
        q = Select('t1')
        sql_t = q.columns('t1c1').sql()
        assert_equals(sql_t, ('SELECT `t1c1` FROM t1', None))

    def test_select_nocol(self):
        q = Select()
        sql_t = q.from_table('t1').sql()
        assert_equals(sql_t, ('SELECT * FROM t1', None))

    def test_select_col(self):
        q = Select()
        sql_t = q.from_table('t1').columns('t1c1').sql()
        assert_equals(sql_t, ('SELECT `t1c1` FROM t1', None))

    def test_select_col_qualified(self):
        q = Select()
        sql_t = q.from_table('t1').columns('t1.t1c1').sql()
        assert_equals(sql_t, ('SELECT t1.`t1c1` FROM t1', None))

    def test_select_col_qualified_noquotes(self):
        q = Select(quote_all_col_refs = False)
        sql_t = q.from_table('t1').columns('t1.t1c1').sql()
        assert_equals(sql_t, ('SELECT t1.t1c1 FROM t1', None))

    def test_select_col_tableas(self):
        q = Select()
        sql_t = q.from_table('t1 AS t1a').columns('t1c1').sql()
        assert_equals(sql_t, ('SELECT `t1c1` FROM t1 AS t1a', None))

    def test_select_col_callable(self):
        q = Select()
        sql_t = q.from_table('t1').columns('t1c1')()
        assert_equals(sql_t, ('SELECT `t1c1` FROM t1', None))

    def test_select_cols_fn(self):
        q = Select()
        sql_t = q.from_table('t1').column('t1c1').column('t1c2').sql()
        assert_equals(sql_t, ('SELECT `t1c1`, `t1c2` FROM t1', None))

    def test_select_cols_list(self):
        q = Select()
        sql_t = q.from_table('t1').columns(['t1c1','t1c2']).sql()
        assert_equals(sql_t, ('SELECT `t1c1`, `t1c2` FROM t1', None))

    def test_select_cols_tuple(self):
        q = Select()
        sql_t = q.from_table('t1').columns(('t1c1','t1c2')).sql()
        assert_equals(sql_t, ('SELECT `t1c1`, `t1c2` FROM t1', None))

    def test_select_no_table(self):
        q = Select()
        sql_t = q.column_expr('1+1').sql()
        assert_equals(sql_t, ('SELECT 1+1', None))

    def test_select_expr_with_param(self):
        q = Select()
        data = ['mypw']
        sql_t = q.column_expr('PASSWORD(?)', data).sql()
        assert_equals(sql_t, ('SELECT PASSWORD(?)', data))

    def test_select_distinct(self):
        q = Select('t1')
        sql_t = q.set_option('DISTINCT').columns('t1c1').sql()
        assert_equals(sql_t, ('SELECT DISTINCT `t1c1` FROM t1', None))

    def test_select_quote_col_prequoted(self):
        q = Select()
        sql_t = q.from_table('t1').columns('`t1c1`').sql()
        assert_equals(sql_t, ('SELECT `t1c1` FROM t1', None))

    def test_select_quote_col_func(self):
        q = Select()
        sql_t = q.from_table('t1').columns('DATE(`t1c1`)').sql()
        assert_equals(sql_t, ('SELECT DATE(`t1c1`) FROM t1', None))

    def test_select_quote_col_as(self):
        q = Select()
        sql_t = q.from_table('t1').columns('t1c1 AS t1a1').sql()
        assert_equals(sql_t, ('SELECT t1c1 AS t1a1 FROM t1', None))

    def test_join_field(self):
        # join(table, 'Field1')
        # JOIN table USING (Field1)
        q = Select()
        sql_t = q.columns(['t1c1', 't2c1']).from_table('t1').left_join('t2', 't1c1').sql()
        assert_equals(sql_t, ('SELECT `t1c1`, `t2c1` FROM t1 LEFT JOIN t2 USING (`t1c1`)', None))

    def test_join_root_field(self):
        # join(table, '.Field1')
        # JOIN table ON (root_table.Field1 = table.Field1)
        q = Select()
        sql_t = q.columns(['t1c1', 't2c1']).from_table('t1').left_join('t2', '.t1c1').sql()
        assert_equals(sql_t, ('SELECT `t1c1`, `t2c1` FROM t1 LEFT JOIN t2 ON (t1.`t1c1` = t2.`t1c1`)', None))

    def test_join_field_list(self):
        # when join_cond is ('Field1', 'Field2', ...)
        # JOIN table USING (Field1, Field2, ...)
        q = Select()
        sql_t = q.columns(['t1c1', 't2c1']).from_table('t1').left_join('t2', ('t1c1','t2c1')).sql()
        assert_equals(sql_t, ('SELECT `t1c1`, `t2c1` FROM t1 LEFT JOIN t2 USING (`t1c1`, `t2c1`)', None))

    def test_join_fields(self):
        # join(table, (.Field1, .Field2))
        # JOIN table ON (root_table.Field1 = table.Field2)
        q = Select()
        sql_t = q.columns(['t1c1', 't2c1']).from_table('t1').left_join('t2', ('.t1c1','.t2c1')).sql()
        assert_equals(sql_t, ('SELECT `t1c1`, `t2c1` FROM t1 LEFT JOIN t2 ON (t1.`t1c1` = t2.`t2c1`)', None))

    def test_join_condition(self):
        # join(table, [condition,...])
        # JOIN table ON (condition [AND condition [AND ...]])
        q = Select()
        sql_t = q.columns(['t1c1', 't2c1']).from_table('t1').left_join('t2', ['t1c1 = t2c1']).sql()
        assert_equals(sql_t, ('SELECT `t1c1`, `t2c1` FROM t1 LEFT JOIN t2 ON (t1c1 = t2c1)', None))

    def test_join_conditions(self):
        # join(table, [condition,...])
        # JOIN table ON (condition [AND condition [AND ...]])
        q = Select()
        sql_t = q.columns(['t1c1', 't2c1']).from_table('t1').left_join('t2', ['t1c1 = t2c1']).sql()
        assert_equals(sql_t, ('SELECT `t1c1`, `t2c1` FROM t1 LEFT JOIN t2 ON (t1c1 = t2c1)', None))

    def test_join_fields_qualified(self):
        # join(table, (.Field1, .Field2))
        # JOIN table ON (root_table.Field1 = table.Field2)
        q = Select()
        sql_t = q.columns(['t1.t1c1', 't2.t2c1']).from_table('t1').left_join('t2', ('.t1c1','.t2c1')).sql()
        assert_equals(sql_t, ('SELECT t1.`t1c1`, t2.`t2c1` FROM t1 LEFT JOIN t2 ON (t1.`t1c1` = t2.`t2c1`)', None))

    def test_straight_join_field(self):
        # join(table, 'Field1')
        # JOIN table USING (Field1)
        q = Select()
        sql_t = q.columns(['t1c1', 't2c1']).from_table('t1').join('t2', 't1c1', 'STRAIGHT_JOIN').sql()
        assert_equals(sql_t, ('SELECT `t1c1`, `t2c1` FROM t1 STRAIGHT_JOIN t2 USING (`t1c1`)', None))

    def test_join_fields_tableas(self):
        # join(table, (.Field1, .Field2))
        # JOIN table ON (root_table.Field1 = table.Field2)
        q = Select()
        sql_t = q.columns(['t1a.c1', 't2a.c1']).from_table('t1 AS t1a').left_join('t2 AS t2a', ('.t1c1','.t2c1')).sql()
        assert_equals(sql_t, ('SELECT t1a.`c1`, t2a.`c1` FROM t1 AS t1a LEFT JOIN t2 AS t2a ON (t1a.`t1c1` = t2a.`t2c1`)', None))

    def test_join_root_field_tableas(self):
        # join(table, '.Field1')
        # JOIN table ON (root_table.Field1 = table.Field1)
        q = Select()
        sql_t = q.columns(['t1a.c1', 't2a.c1']).from_table('t1 AS t1a').left_join('t2 AS t2a', '.t1c1').sql()
        assert_equals(sql_t, ('SELECT t1a.`c1`, t2a.`c1` FROM t1 AS t1a LEFT JOIN t2 AS t2a ON (t1a.`t1c1` = t2a.`t1c1`)', None))

    def test_join_root_field_multi(self):
        # join(table, '.Field1')
        # JOIN table ON (root_table.Field1 = table.Field1)
        q = Select()
        sql_t = q.columns(['t1c1', 't2c1', 't3c1']).from_table('t1').left_join('t2', '.t1c1').left_join('t3', '.t1c1').sql()
        assert_equals(sql_t, ('SELECT `t1c1`, `t2c1`, `t3c1` FROM t1 LEFT JOIN t2 ON (t1.`t1c1` = t2.`t1c1`) LEFT JOIN t3 ON (t1.`t1c1` = t3.`t1c1`)', None))

    def test_join_root_field_multi_dict(self):
        # join(table, '.Field1')
        # JOIN table ON (root_table.Field1 = table.Field1)
        q = Select()
        sql_t = q.columns(['t1c1', 't2c1', 't3c1']).from_table('t1').left_join(OrderedDict([('t2', '.t1c1'),('t3','.t1c1')])).sql()
        assert_equals(sql_t, ('SELECT `t1c1`, `t2c1`, `t3c1` FROM t1 LEFT JOIN t2 ON (t1.`t1c1` = t2.`t1c1`) LEFT JOIN t3 ON (t1.`t1c1` = t3.`t1c1`)', None))

    def test_join_root_field_multi_dotdot(self):
        # join(table, '..Field1')
        # JOIN table ON (previous_join_table.Field1 = table.Field1)
        q = Select()
        sql_t = q.columns(['t1c1', 't2c1', 't3c1']).from_table('t1').left_join('t2', '..t1c1').left_join('t3', '..t2c1').sql()
        assert_equals(sql_t, ('SELECT `t1c1`, `t2c1`, `t3c1` FROM t1 LEFT JOIN t2 ON (t1.`t1c1` = t2.`t1c1`) LEFT JOIN t3 ON (t2.`t2c1` = t3.`t2c1`)', None))

    def test_where_value(self):
        q = Select()
        sql_t = q.from_table('t1').where_value('t1c1', 3).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE `t1c1` = 3', None))

    def test_where_values(self):
        q = Select()
        sql_t = q.from_table('t1').where_value('t1c1', 3).where_value('t1c2', 'string').sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE (`t1c1` = 3 AND `t1c2` = ?)', ['string']))

    def test_where_value_or(self):
        q = Select()
        sql_t = q.from_table('t1').where_or().where_value('t1c1', 3).where_value('t1c1', 5).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE (`t1c1` = 3 OR `t1c1` = 5)', None))

    def test_where_value_and_or_default(self):
        q = Select()
        sql_t = q.from_table('t1').where_value('t1c1', 1).where_value('t1c2', 5).where_and().where_value('t1c1', 6).where_value('t1c2', 10).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE ((`t1c1` = 1 AND `t1c2` = 5) OR (`t1c1` = 6 AND `t1c2` = 10))', None))

    def test_where_value_and_or(self):
        q = Select()
        sql_t = q.from_table('t1').where_and().where_value('t1c1', 1).where_value('t1c2', 5).where_and().where_value('t1c1', 6).where_value('t1c2', 10).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE ((`t1c1` = 1 AND `t1c2` = 5) OR (`t1c1` = 6 AND `t1c2` = 10))', None))

    def test_where_value_or_and(self):
        q = Select(where_predicate='AND')
        sql_t = q.from_table('t1').where_or().where_value('t1c1', 1).where_value('t1c1', 5).where_or().where_value('t1c1', 6).where_value('t1c1', 10).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE ((`t1c1` = 1 OR `t1c1` = 5) AND (`t1c1` = 6 OR `t1c1` = 10))', None))

    def test_where_values_dict(self):
        q = Select()
        sql_t = q.from_table('t1').where_value(OrderedDict([('t1c1', 3), ('t1c2', 'string')])).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE (`t1c1` = 3 AND `t1c2` = ?)', ['string']))

    def test_where_values_noparam(self):
        q = Select(placeholder=False)
        sql = q.from_table('t1').where_value('t1c1', 3).where_value('t1c2', "'string'").sql()
        assert_equals(sql, "SELECT * FROM t1 WHERE (`t1c1` = 3 AND `t1c2` = 'string')")

    def test_where_raw_value(self):
        q = Select()
        sql_t = q.from_table('t1').where_raw_value('t1c1', 'NOW()').sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE `t1c1` = NOW()', None))

    def test_where_raw_values(self):
        q = Select()
        sql_t = q.from_table('t1').where_raw_value(OrderedDict([('t1c1', 'NOW()'), ('t1c2', 'NOW()')])).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE (`t1c1` = NOW() AND `t1c2` = NOW())', None))

    def test_where_raw_value_with_param(self):
        q = Select()
        sql_t = q.from_table('t1').where_raw_value('t1c1', 'PASSWORD(?)', value_params=('mypw',)).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE `t1c1` = PASSWORD(?)', ['mypw']))

    def test_where_raw_values_with_param(self):
        q = Select()
        sql_t = q.from_table('t1').where_raw_value('t1c1', 'PASSWORD(?)', value_params=('mypw1',)).where_raw_value('t1c2', 'PASSWORD(?)', value_params=('mypw2',)).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE (`t1c1` = PASSWORD(?) AND `t1c2` = PASSWORD(?))', ['mypw1', 'mypw2']))

    def test_where_raw_value_with_param_between(self):
        between_dates = (datetime.date(2014, 3, 2), datetime.date(2014, 3, 12))
        q = Select()
        sql_t = q.from_table('t1').where_raw_value('DATE(`t1c1`)', '? AND ?', 'BETWEEN', between_dates).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE DATE(`t1c1`) BETWEEN ? AND ?', ['2014-03-02', '2014-03-12']))

    def test_where_raw_value_func(self):
        q = Select()
        sql_t = q.from_table('t1').where_raw_value('DATE(t1c1)', 'NOW()', '>').sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE DATE(t1c1) > NOW()', None))

    def test_where_value_true(self):
        q = Select()
        sql_t = q.from_table('t1').where_value('t1c1', True).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE `t1c1` = 1', None))

    def test_where_value_false(self):
        q = Select()
        sql_t = q.from_table('t1').where_value('t1c1', False).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE `t1c1` = 0', None))

    def test_where_value_datetime(self):
        q = Select()
        sql_t = q.from_table('t1').where_value('t1c1', datetime.datetime(2014,3,2,12,01,02)).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE `t1c1` = ?', ['2014-03-02 12:01:02']))

    def test_where_value_date(self):
        q = Select()
        sql_t = q.from_table('t1').where_value('t1c1', datetime.date(2014,3,2)).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE `t1c1` = ?', ['2014-03-02']))

    def test_where_value_time(self):
        q = Select()
        sql_t = q.from_table('t1').where_value('t1c1', datetime.time(12,01,02)).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE `t1c1` = ?', ['12:01:02']))

    def test_where_value_date_func(self):
        q = Select()
        sql_t = q.from_table('t1').where_value('DATE(`t1c1`)', datetime.date(2014,3,2), '>').sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE DATE(`t1c1`) > ?', ['2014-03-02']))

    def test_where_value_object(self):
        class TestClass(object):
            def __str__(self):
                return "object as a string"
        q = Select()
        sql_t = q.from_table('t1').where_value('t1c1', TestClass()).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE `t1c1` = ?', ['object as a string']))

    def test_where_expr(self):
        q = Select()
        sql_t = q.from_table('t1').where_expr('`t1c1` = NOW()').sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE `t1c1` = NOW()', None))

    def test_where_exprs(self):
        q = Select()
        sql_t = q.from_table('t1').where_expr(['`t1c1` = NOW()', '`t1c2` = NOW()']).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE (`t1c1` = NOW() AND `t1c2` = NOW())', None))

    def test_where_expr_with_expr_param(self):
        q = Select()
        sql_t = q.from_table('t1').where_expr('`t1c1` = PASSWORD(?)', ('mypw',)).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE `t1c1` = PASSWORD(?)', ['mypw']))

    def test_where_value_null(self):
        q = Select()
        sql_t = q.from_table('t1').where_value('t1c1', None).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE `t1c1` IS NULL', None))

    def test_where_value_notnull(self):
        q = Select()
        sql_t = q.from_table('t1').where_value('t1c1', None, '<>').sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE `t1c1` IS NOT NULL', None))

    def test_where_value_notnull_tuple(self):
        q = Select()
        sql_t = q.from_table('t1').where_value('t1c1', (None, '<>')).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE `t1c1` IS NOT NULL', None))

    def test_where_value_in(self):
        q = Select()
        sql_t = q.from_table('t1').where_value('t1c1', [1,2,3]).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE `t1c1` IN (1, 2, 3)', None))

    def test_where_value_notin(self):
        q = Select()
        sql_t = q.from_table('t1').where_value('t1c1', [1,2,3], '<>').sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE `t1c1` NOT IN (1, 2, 3)', None))

    def test_where_value_in_single(self):
        q = Select()
        sql_t = q.from_table('t1').where_value('t1c1', [1]).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE `t1c1` = 1', None))

    def test_where_value_notin_single(self):
        q = Select()
        sql_t = q.from_table('t1').where_value('t1c1', [1], '<>').sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE `t1c1` <> 1', None))

    def test_where_value_in_single_alt(self):
        q = Select()
        sql_t = q.from_table('t1').where_value('t1c1', [1], 'IN').sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE `t1c1` = 1', None))

    def test_where_value_notin_single_alt(self):
        q = Select()
        sql_t = q.from_table('t1').where_value('t1c1', [1], 'NOT IN').sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE `t1c1` <> 1', None))

    def test_where_value_like(self):
        q = Select()
        sql_t = q.from_table('t1').where_value('t1c1', 'abc%', 'LIKE').sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE `t1c1` LIKE ?', ['abc%']))

    def test_where_value_notlike(self):
        q = Select()
        sql_t = q.from_table('t1').where_value('t1c1', 'abc%', 'NOT LIKE').sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE `t1c1` NOT LIKE ?', ['abc%']))

    def test_where_raw_value_between(self):
        q = Select()
        sql_t = q.from_table('t1').where_raw_value('t1c1', '1 AND 5', 'BETWEEN').sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE `t1c1` BETWEEN 1 AND 5', None))

    def test_where_raw_value_between_tuple_param(self):
        q = Select()
        sql_t = q.from_table('t1').where_raw_value('t1c1', '? AND ?', 'BETWEEN', value_params=('a', 'b')).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE `t1c1` BETWEEN ? AND ?', ['a', 'b']))

    def test_join_using_where_expr(self):
        q = Select()
        sql_t = q.from_table(['t1', 't2']).where_expr('(t1.t1c1 = t2.t2c1)').sql()
        assert_equals(sql_t, ('SELECT * FROM t1, t2 WHERE (t1.t1c1 = t2.t2c1)', None))

    def test_having_value(self):
        q = Select()
        sql_t = q.from_table('t1').having_value('t1c1', 3).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 HAVING `t1c1` = 3', None))

    def test_having_value_or(self):
        q = Select()
        sql_t = q.from_table('t1').having_or().having_value('t1c1', 3).having_value('t1c1', 5).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 HAVING (`t1c1` = 3 OR `t1c1` = 5)', None))

    def test_having_value_and_or_default(self):
        q = Select()
        sql_t = q.from_table('t1').having_value('t1c1', 1).having_value('t1c2', 5).having_and().having_value('t1c1', 6).having_value('t1c2', 10).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 HAVING ((`t1c1` = 1 AND `t1c2` = 5) OR (`t1c1` = 6 AND `t1c2` = 10))', None))

    def test_having_value_and_or(self):
        q = Select()
        sql_t = q.from_table('t1').having_and().having_value('t1c1', 1).having_value('t1c2', 5).having_and().having_value('t1c1', 6).having_value('t1c2', 10).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 HAVING ((`t1c1` = 1 AND `t1c2` = 5) OR (`t1c1` = 6 AND `t1c2` = 10))', None))

    def test_having_value_or_and(self):
        q = Select(having_predicate='AND')
        sql_t = q.from_table('t1').having_or().having_value('t1c1', 1).having_value('t1c1', 5).having_or().having_value('t1c1', 6).having_value('t1c1', 10).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 HAVING ((`t1c1` = 1 OR `t1c1` = 5) AND (`t1c1` = 6 OR `t1c1` = 10))', None))

    def test_having_values(self):
        q = Select()
        sql_t = q.from_table('t1').having_value('t1c1', 3).having_value('t1c2', 'string').sql()
        assert_equals(sql_t, ('SELECT * FROM t1 HAVING (`t1c1` = 3 AND `t1c2` = ?)', ['string']))

    def test_having_values_dict(self):
        q = Select()
        sql_t = q.from_table('t1').having_value(OrderedDict([('t1c1', 3), ('t1c2', 'string')])).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 HAVING (`t1c1` = 3 AND `t1c2` = ?)', ['string']))

    def test_having_raw_value(self):
        q = Select()
        sql_t = q.from_table('t1').having_raw_value('t1c1', 'NOW()').sql()
        assert_equals(sql_t, ('SELECT * FROM t1 HAVING `t1c1` = NOW()', None))

    def test_having_raw_value_with_param(self):
        q = Select()
        sql_t = q.from_table('t1').having_raw_value('t1c1', 'PASSWORD(?)', value_params=('mypw',)).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 HAVING `t1c1` = PASSWORD(?)', ['mypw']))

    def test_having_expr(self):
        q = Select()
        sql_t = q.from_table('t1').having_expr('`t1c1` = NOW()').sql()
        assert_equals(sql_t, ('SELECT * FROM t1 HAVING `t1c1` = NOW()', None))

    def test_having_expr_with_expr_param(self):
        q = Select()
        sql_t = q.from_table('t1').having_expr('`t1c1` = PASSWORD(?)', ('mypw',)).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 HAVING `t1c1` = PASSWORD(?)', ['mypw']))

    def test_select_orderby(self):
        q = Select()
        sql_t = q.from_table('t1').order_by('t1c1').sql()
        assert_equals(sql_t, ('SELECT * FROM t1 ORDER BY t1c1', None))

    def test_select_groupby(self):
        q = Select()
        sql_t = q.from_table('t1').group_by('t1c1').sql()
        assert_equals(sql_t, ('SELECT * FROM t1 GROUP BY t1c1', None))

    def test_select_groupbys(self):
        q = Select()
        sql_t = q.from_table('t1').group_by(['t1c1', 't1c2']).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 GROUP BY t1c1, t1c2', None))

    def test_select_limit(self):
        q = Select()
        sql_t = q.from_table('t1').limit(5).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 LIMIT 5', None))

    def test_select_limit_offset(self):
        q = Select()
        sql_t = q.from_table('t1').limit(10, 5).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 LIMIT 5,10', None))

    def test_where_and_multi(self):
        q = Select()
        sql_t = q.from_table('t1').where_value('t1c1', 1).where_and().where_value('t1c2', 5).where_and().where_value('t1c1', 6).sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE (`t1c1` = 1 OR `t1c2` = 5 OR `t1c1` = 6)', None))

    def test_cacheable(self):
        q = Select(cacheable=True)
        sql_t = q.from_table('t1').columns('t1c1').sql()
        assert_equals(sql_t, ('SELECT SQL_CACHE `t1c1` FROM t1', None))

    def test_not_cacheable(self):
        q = Select(cacheable=False)
        sql_t = q.from_table('t1').columns('t1c1').sql()
        assert_equals(sql_t, ('SELECT SQL_NO_CACHE `t1c1` FROM t1', None))

    def test_where_value_utf_param(self):
        q = Select()
        sql_t = q.from_table('t1').where_value('t1c1', u"äöü").sql()
        assert_equals(sql_t, ('SELECT * FROM t1 WHERE `t1c1` = ?', [u"äöü"]))

    def test_where_value_utf_inline(self):
        q = Select()
        sql_t = q.from_table('t1').where_expr(u'`t1c1` = "äöü"').sql()
        assert_equals(sql_t, (u'SELECT * FROM t1 WHERE `t1c1` = "äöü"', None))

    def test_where_value_utf_noparam(self):
        q = Select(placeholder=False)
        sql = q.from_table('t1').where_value('t1c1', u'"äöü"').sql()
        assert_equals(sql, u'SELECT * FROM t1 WHERE `t1c1` = "äöü"')

    def test_remove_col(self):
        q = Select()
        sql_t = q.from_table('t1').columns(('t1c1','t2c1')).remove_column('t2c1').sql()
        assert_equals(sql_t, ('SELECT `t1c1` FROM t1', None))

    def test_remove_col_expr(self):
        q = Select()
        sql_t = q.from_table('t1').columns('t1c1').column_expr('1+1 AS t2c1').remove_column('t2c1').sql()
        assert_equals(sql_t, ('SELECT `t1c1` FROM t1', None))

    def test_qualify_columns(self):
        q = Select()
        sql_t = q.from_table('t1').columns(('t1c1', 't1c2')).qualify_columns('t1', ('t1c1',)).sql()
        assert_equals(sql_t, ('SELECT t1.`t1c1`, `t1c2` FROM t1', None))

    def test_qualify_columns_all(self):
        q = Select()
        sql_t = q.from_table('t1').columns(('t1c1', 't1c2')).qualify_columns('t1').sql()
        assert_equals(sql_t, ('SELECT t1.`t1c1`, t1.`t1c2` FROM t1', None))

    def test_qualify_columns_all_nonqual(self):
        q = Select()
        sql_t = q.from_table(('t1', 't2')).columns(('t1c1', 't2.t2c1')).qualify_columns('t1').sql()
        assert_equals(sql_t, ('SELECT t1.`t1c1`, t2.`t2c1` FROM t1, t2', None))

    @raises(ValueError)
    def test_join_no_root(self):
        q = Select()
        q.columns(['t1c1', 't2c1']).left_join('t2', 't1c1').sql()
