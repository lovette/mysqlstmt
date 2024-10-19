import unittest
from collections import OrderedDict

from nose.tools import assert_equals, raises

from mysqlstmt import Delete


class TestDelete(unittest.TestCase):
    def test_constructor_table_name(self):
        q = Delete('t1')
        sql_t = q.where_value('t1c1', 1).sql()
        assert_equals(sql_t, ('DELETE FROM t1 WHERE `t1c1` = 1', None))

    def test_delete_int(self):
        q = Delete()
        sql_t = q.from_table('t1').where_value('t1c1', 1).sql()
        assert_equals(sql_t, ('DELETE FROM t1 WHERE `t1c1` = 1', None))

    def test_delete_int_callable(self):
        q = Delete()
        sql_t = q.from_table('t1').where_value('t1c1', 1)()
        assert_equals(sql_t, ('DELETE FROM t1 WHERE `t1c1` = 1', None))

    def test_delete_ints(self):
        q = Delete()
        sql_t = q.from_table('t1').where_value('t1c1', 1).where_value('t1c2', 2).sql()
        assert_equals(sql_t, ('DELETE FROM t1 WHERE (`t1c1` = 1 AND `t1c2` = 2)', None))

    def test_dict_int(self):
        q = Delete()
        values = {'t1c1': 1}
        sql_t = q.from_table('t1').where_value(values).sql()
        assert_equals(sql_t, ('DELETE FROM t1 WHERE `t1c1` = 1', None))

    def test_dict_ints(self):
        q = Delete()
        values = OrderedDict([('t1c1', 1), ('t1c2', 2)])
        sql_t = q.from_table('t1').where_value(values).sql()
        assert_equals(sql_t, ('DELETE FROM t1 WHERE (`t1c1` = 1 AND `t1c2` = 2)', None))

    def test_dict_strings(self):
        q = Delete()
        values = OrderedDict([('t1c1', 'a'), ('t1c2', 'b')])
        sql_t = q.from_table('t1').where_value(values).sql()
        assert_equals(sql_t, ("DELETE FROM t1 WHERE (`t1c1` = ? AND `t1c2` = ?)", ['a', 'b']))

    def test_null(self):
        q = Delete()
        values = OrderedDict([('t1c1', 'a'), ('t1c2', None)])
        sql_t = q.from_table('t1').where_value(values).sql()
        assert_equals(sql_t, ("DELETE FROM t1 WHERE (`t1c1` = ? AND `t1c2` IS NULL)", ['a']))

    def test_function_value(self):
        q = Delete()
        sql_t = q.from_table('t1').where_value('t1c1', 'NOW()').sql()
        assert_equals(sql_t, ('DELETE FROM t1 WHERE `t1c1` = ?', ['NOW()']))

    def test_function_raw_value(self):
        q = Delete()
        sql_t = q.from_table('t1').where_raw_value('t1c1', 't1c1+1').sql()
        assert_equals(sql_t, ('DELETE FROM t1 WHERE `t1c1` = t1c1+1', None))

    def test_function_raw_value_dict(self):
        q = Delete()
        sql_t = q.from_table('t1').where_raw_value({'t1c1': 'NOW()'}).sql()
        assert_equals(sql_t, ('DELETE FROM t1 WHERE `t1c1` = NOW()', None))

    def test_order_by(self):
        q = Delete()
        sql_t = q.from_table('t1').where_value('t1c1', 1).order_by('t1c2').sql()
        assert_equals(sql_t, ('DELETE FROM t1 WHERE `t1c1` = 1 ORDER BY t1c2', None))

    def test_order_bys(self):
        q = Delete()
        sql_t = q.from_table('t1').where_value('t1c1', 1).order_by(['t1c1', 't1c2']).sql()
        assert_equals(sql_t, ('DELETE FROM t1 WHERE `t1c1` = 1 ORDER BY t1c1, t1c2', None))

    def test_limit(self):
        q = Delete()
        sql_t = q.from_table('t1').where_value('t1c1', 1).limit(5).sql()
        assert_equals(sql_t, ('DELETE FROM t1 WHERE `t1c1` = 1 LIMIT 5', None))

    def test_ignore(self):
        q = Delete(ignore_error=True)
        sql_t = q.from_table('t1').where_value('t1c1', 1).sql()
        assert_equals(sql_t, ('DELETE IGNORE FROM t1 WHERE `t1c1` = 1', None))

    def test_unqualified(self):
        q = Delete(allow_unqualified_delete=True)
        sql_t = q.from_table('t1').sql()
        assert_equals(sql_t, ('DELETE FROM t1', None))

    def test_noparam(self):
        q = Delete(placeholder=False)
        sql = q.from_table('t1').where_value('t1c1', 1).limit(5).sql()
        assert_equals(sql, 'DELETE FROM t1 WHERE `t1c1` = 1 LIMIT 5')

    def test_join_table(self):
        q = Delete('t1')
        sql_t = q.join('t2', 't2c1').where_value('t2.t2c2', 1).where_value('t2.t2c3', 2).sql()
        assert_equals(sql_t, ('DELETE FROM t1 USING t1 INNER JOIN t2 USING (`t2c1`) WHERE (t2.`t2c2` = 1 AND t2.`t2c3` = 2)', None))

    def test_multi_table(self):
        q = Delete(('t1', 't2'), allow_unqualified_delete=True)
        sql_t = q.join('t2', '.t1c1').join('t3', '..t1c1').sql()
        assert_equals(sql_t, ('DELETE FROM t1, t2 USING t1 INNER JOIN t2 ON (t1.`t1c1` = t2.`t1c1`) INNER JOIN t3 ON (t2.`t1c1` = t3.`t1c1`)', None))

    def test_delete_option(self):
        q = Delete()
        sql_t = q.set_option('LOW_PRIORITY').from_table('t1').where_value('t1c1', 1).sql()
        assert_equals(sql_t, ('DELETE LOW_PRIORITY FROM t1 WHERE `t1c1` = 1', None))

    @raises(ValueError)
    def test_fail_no_table(self):
        q = Delete()
        q.where_value('t1c1', 1).sql()

    @raises(ValueError)
    def test_fail_unqualified(self):
        q = Delete()
        q.from_table('t1').sql()
