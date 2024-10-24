"""mysqlstmt where_mixin class module.

This module provides:
- WhereMixin
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .where_condition import WhereCondition

if TYPE_CHECKING:
    from collections.abc import Sequence

    from .stmt import Stmt


class WhereMixin:
    """Provide where_value related functionality to statement classes.

    Note:
        This class is not to be instantiated directly.
    """

    def __init__(self, where_predicate: str = "OR", **kwargs) -> None:
        """Constructor.

        Keyword Arguments:
            where_predicate (string, optional): The predicate for the outer WHERE condition, either 'AND' or 'OR'.
            **kwargs: Base class arguments.
        """
        super().__init__(**kwargs)

        assert where_predicate in ("AND", "OR")

        self._where_cond_root = WhereCondition(self, where_predicate=where_predicate)

        # Default first condition is 'AND'; will be ignored if where_or is called first
        self.where_cond(where_predicate="AND")

    def where_value(self, field_or_dict: str | dict, value_or_tuple: str | Sequence | None = None, operator: str = "=") -> Stmt:
        """Compare field to a value.

        Field names may be escaped with backticks.
        Use :py:meth:`where_expr` if you want field names to be
        included in the SQL statement verbatim.

        Values will be pickled by :py:meth:`mysqlstmt.stmt.Stmt.pickle`.
        Use :py:meth:`where_raw_value` if you want values to be
        included in the SQL statement verbatim.

        Arguments:
            field_or_dict (string or list): Name of field/column or :py:class:`dict` mapping fields to values.
            value_or_tuple (mixed or tuple, optional): Value to compare with if ``field_or_dict`` is a field name.
                Type can be anything that :py:meth:`mysqlstmt.stmt.Stmt.pickle` can handle (Iterable, Object,etc.).
                Can also be a tuple ``(value, operator)``.
            operator (string, optional): Comparison operator, default is '='.

        Returns:
            object: self

        Examples: ::

            >>> q = Select()
            >>> q.from_table('t1').where_value('t1c1', True).sql()
            ('SELECT * FROM t1 WHERE `t1c1` = 1', None)

            >>> q = Select()
            >>> q.from_table('t1').where_value('t1c1', False).sql()
            ('SELECT * FROM t1 WHERE `t1c1` = 0', None)

            >>> q = Select()
            >>> q.from_table('t1').where_value('t1c1', datetime.datetime(2014,3,2,12,01,02)).sql()
            ('SELECT * FROM t1 WHERE `t1c1` = ?', ['2014-03-02 12:01:02'])

            >>> q = Select()
            >>> q.from_table('t1').where_value('t1c1', datetime.date(2014,3,2)).sql()
            ('SELECT * FROM t1 WHERE `t1c1` = ?', ['2014-03-02'])

            >>> q = Select()
            >>> q.from_table('t1').where_value('t1c1', datetime.time(12,01,02)).sql()
            ('SELECT * FROM t1 WHERE `t1c1` = ?', ['12:01:02'])

            >>> q = Select()
            >>> q.from_table('t1').where_value('t1c1', None).sql()
            ('SELECT * FROM t1 WHERE `t1c1` IS NULL', None)

            >>> q = Select()
            >>> q.from_table('t1').where_value('t1c1', None, '<>').sql()
            ('SELECT * FROM t1 WHERE `t1c1` IS NOT NULL', None)

            >>> q = Select()
            >>> q.from_table('t1').where_value('t1c1', (None, '<>')).sql()
            ('SELECT * FROM t1 WHERE `t1c1` IS NOT NULL', None)

            >>> q = Select()
            >>> q.from_table('t1').where_value('t1c1', [1,2,3]).sql()
            ('SELECT * FROM t1 WHERE `t1c1` IN (1, 2, 3)', None)

            >>> q = Select()
            >>> q.from_table('t1').where_value('t1c1', [1,2,3], '<>').sql()
            ('SELECT * FROM t1 WHERE `t1c1` NOT IN (1, 2, 3)', None)

            >>> q = Select()
            >>> q.from_table('t1').where_value('t1c1', [1]).sql()
            ('SELECT * FROM t1 WHERE `t1c1` = 1', None)

            >>> q = Select()
            >>> q.from_table('t1').where_value('t1c1', [1], '<>').sql()
            ('SELECT * FROM t1 WHERE `t1c1` <> 1', None)

            >>> q = Select()
            >>> q.from_table('t1').where_value('t1c1', [1], 'IN').sql()
            ('SELECT * FROM t1 WHERE `t1c1` = 1', None)

            >>> q = Select()
            >>> q.from_table('t1').where_value('t1c1', [1], 'NOT IN').sql()
            ('SELECT * FROM t1 WHERE `t1c1` <> 1', None)

            >>> q = Select()
            >>> q.from_table('t1').where_value('t1c1', 'abc%', 'LIKE').sql()
            ('SELECT * FROM t1 WHERE `t1c1` LIKE ?', ['abc%'])

            >>> q = Select()
            >>> q.from_table('t1').where_value('t1c1', 'abc%', 'NOT LIKE').sql()
            ('SELECT * FROM t1 WHERE `t1c1` NOT LIKE ?', ['abc%'])

            >>> q = Select()
            >>> q.from_table('t1').where_value('t1c1', 3).sql()
            ('SELECT * FROM t1 WHERE `t1c1` = 3', None)

            >>> q = Select()
            >>> q.from_table('t1').where_value('t1c1', 3).where_value('t1c2', 'string').sql()
            ('SELECT * FROM t1 WHERE (`t1c1` = 3 AND `t1c2` = ?)', ['string'])

            >>> q = Select()
            >>> q.from_table('t1').where_value(OrderedDict([('t1c1', 3), ('t1c2', 'string')])).sql()
            ('SELECT * FROM t1 WHERE (`t1c1` = 3 AND `t1c2` = ?)', ['string'])

            >>> q = Select(placeholder=False)
            >>> q.from_table('t1').where_value('t1c1', 3).where_value('t1c2', "'string'").sql()
            "SELECT * FROM t1 WHERE (`t1c1` = 3 AND `t1c2` = 'string')"

            >>> q = Select()
            >>> q.from_table('t1').where_value('DATE(`t1c1`)', datetime.date(2014,3,2), '>').sql()
            ('SELECT * FROM t1 WHERE DATE(`t1c1`) > ?', ['2014-03-02'])
        """
        self.get_where_cond().where_value(field_or_dict, value_or_tuple, operator)
        return self

    where_values = where_value

    def where_raw_value(
        self,
        field_or_dict: str | dict,
        value_or_tuple: str | Sequence | None = None,
        operator: str = "=",
        value_params: Sequence | None = None,
    ) -> Stmt:
        """Compare field to a an unmanipulated value.

        Field names may be escaped with backticks.
        Use :py:meth:`where_expr` if you want field names to be
        included in the SQL statement verbatim.

        Values will be included in the SQL statement verbatim.
        Use :py:meth:`where_value` if you want values to be pickled.

        Arguments:
            field_or_dict (string or list): Name of field/column or :py:class:`dict` mapping fields to values.
                Dictionary values can also be a tuple, as described below.
            value_or_tuple (string or tuple, optional): Value to compare with if ``field_or_dict`` is a field name.
                Can also be a tuple ``(value, operator, value_params)``.
            operator (string, optional): Comparison operator, default is '='.
            value_params (iterable, optional): List of value params. Default is None.

        Returns:
            object: self

        Examples: ::

            >>> q = Select()
            >>> q.from_table('t1').where_raw_value('t1c1', 'NOW()').sql()
            ('SELECT * FROM t1 WHERE `t1c1` = NOW()', None)

            >>> q = Select()
            >>> q.from_table('t1').where_raw_value(OrderedDict([('t1c1', 'NOW()'), ('t1c2', 'NOW()')])).sql()
            ('SELECT * FROM t1 WHERE (`t1c1` = NOW() AND `t1c2` = NOW())', None)

            >>> q = Select()
            >>> q.from_table('t1').where_raw_value('t1c1', '1 AND 5', 'BETWEEN').sql()
            ('SELECT * FROM t1 WHERE `t1c1` BETWEEN 1 AND 5', None)

            >>> q = Select()
            >>> q.from_table('t1').where_raw_value('t1c1', '? AND ?', 'BETWEEN', value_params=('a', 'b')).sql()
            ('SELECT * FROM t1 WHERE `t1c1` BETWEEN ? AND ?', ['a', 'b'])

            >>> q = Select()
            >>> q.from_table('t1').where_raw_value('t1c1', 'PASSWORD(?)', value_params=('mypw',)).sql()
            ('SELECT * FROM t1 WHERE `t1c1` = PASSWORD(?)', ['mypw'])

            >>> q = Select()
            >>> q.from_table('t1').where_raw_value('DATE(`t1c1`)', 'NOW()', '>').sql()
            ('SELECT * FROM t1 WHERE DATE(`t1c1`) > NOW()')
        """
        self.get_where_cond().where_raw_value(field_or_dict, value_or_tuple, operator, value_params)
        return self

    where_raw_values = where_raw_value

    def where_expr(self, list_or_expr: str | Sequence, expr_params: Sequence | None = None) -> Stmt:
        """Include a complex expression in conditional statement.

        Expressions will be included in the SQL statement verbatim.
        Use :py:meth:`where_value` or :py:meth:`where_raw_value` if you are
        doing basic field comparisons and/or want the value to be pickled.

        Arguments:
            list_or_expr (string or list): An expression or :py:class:`list` of expressions.
                Expression values can also be a tuple ``(expression, expr_params)``.
            expr_params (iterable, optional): List of expression params. Default is None.

        Returns:
            object: self

        Examples: ::

            >>> q = Select()
            >>> q.from_table('t1').where_expr('`t1c1` = NOW()').sql()
            ('SELECT * FROM t1 WHERE `t1c1` = NOW()', None)

            >>> q = Select()
            >>> q.from_table('t1').where_expr(['`t1c1` = NOW()', '`t1c2` = NOW()']).sql()
            ('SELECT * FROM t1 WHERE (`t1c1` = NOW() AND `t1c2` = NOW())', None)

            >>> q = Select()
            >>> q.from_table(['t1', 't2']).where_expr('(t1.t1c1 = t2.t2c1)').sql()
            ('SELECT * FROM t1, t2 WHERE (t1.t1c1 = t2.t2c1)', None)

            >>> q = Select()
            >>> q.from_table('t1').where_expr('`t1c1` = PASSWORD(?)', ('mypw',)).sql()
            ('SELECT * FROM t1 WHERE `t1c1` = PASSWORD(?)', ['mypw'])
        """
        self.get_where_cond().where_expr(list_or_expr, expr_params)
        return self

    where_exprs = where_expr

    def get_where_cond(self, index: int = -1) -> WhereCondition:
        """Returns a ``WhereCondition`` object from the list of conditions.

        Arguments:
            index (int): Index of condition, defaults to the active condition (-1).

        Returns:
            object: :py:class:`WhereCondition`

        Note:
            Conditions are typically created with ``where_and`` and ``where_or``,
            so you should not need to use this function often.

        See Also:
            :py:class:`mysqlstmt.where_condition.WhereCondition` :py:meth:`where_cond`
        """
        return self._where_cond_root.get_where_cond(index)

    def where_cond(self, cond: WhereCondition | None = None, where_predicate: str | None = None) -> Stmt:
        """Activates a new ``WhereCondition``.

        Arguments:
            cond (mysqlstmt.WhereCondition, optional): A new condition; one will be created if not specified.
            where_predicate (string): The predicate for the new condition if a new one is created, either 'AND' or 'OR'.

        Returns:
            object: self

        Note:
            Conditions are typically created with ``where_and`` and ``where_or``.
            You should use this function when creating complex conditions with ``WhereCondition`` objects.

        See Also:
            :py:class:`mysqlstmt.where_condition.WhereCondition` :py:meth:`where_and` :py:meth:`where_or`
        """
        self._where_cond_root.add_cond(cond, where_predicate)
        return self

    def where_and(self) -> Stmt:
        """Activates a new ``WhereCondition`` with an 'AND' predicate.

        Returns:
            object: self

        See Also:
            :py:class:`mysqlstmt.where_condition.WhereCondition` :py:meth:`where_cond` :py:meth:`where_or`

        Examples: ::

            >>> q = Select()
            >>> q.from_table('t1').where_value('t1c1', 1).where_and().where_value(
                't1c2', 5).where_and().where_value('t1c1', 6).sql()
            ('SELECT * FROM t1 WHERE (`t1c1` = 1 OR `t1c2` = 5 OR `t1c1` = 6)', None)

            >>> q = Select()
            >>> q.from_table('t1').where_value('t1c1', 1).where_value(
                't1c2', 5).where_and().where_value('t1c1', 6).where_value('t1c2', 10).sql()
            ('SELECT * FROM t1 WHERE ((`t1c1` = 1 AND `t1c2` = 5) OR (`t1c1` = 6 AND `t1c2` = 10))', None)

            >>> q = Select()
            >>> q.from_table('t1').where_and().where_value('t1c1', 1)
                .where_value('t1c2', 5).where_and().where_value('t1c1', 6).where_value('t1c2', 10).sql()
            ('SELECT * FROM t1 WHERE ((`t1c1` = 1 AND `t1c2` = 5) OR (`t1c1` = 6 AND `t1c2` = 10))', None)
        """
        self._where_cond_root.where_and()
        return self

    def where_or(self) -> Stmt:
        """Activates a new ``WhereCondition`` with an 'OR' predicate.

        Returns:
            object: self

        See Also:
            :py:class:`mysqlstmt.where_condition.WhereCondition` :py:meth:`where_cond` :py:meth:`where_and`

        Examples: ::

            >>> q = Select()
            >>> q.from_table('t1').where_or().where_value('t1c1', 3).where_value('t1c1', 5).sql()
            ('SELECT * FROM t1 WHERE (`t1c1` = 3 OR `t1c1` = 5)', None)

            >>> q = Select(where_predicate='AND')
            >>> q.from_table('t1').where_or().where_value('t1c1', 1).where_value(
                't1c1', 5).where_or().where_value('t1c1', 6).where_value('t1c1', 10).sql()
            ('SELECT * FROM t1 WHERE ((`t1c1` = 1 OR `t1c1` = 5) AND (`t1c1` = 6 OR `t1c1` = 10))', None)
        """
        self._where_cond_root.where_or()
        return self
