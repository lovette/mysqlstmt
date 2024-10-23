"""mysqlstmt join_mixin class module.

This module provides:
- JoinMixin
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from .stmt import Stmt


class JoinMixin:
    """Provide join related functionality to statement classes.

    Note:
        This class is not to be instantiated directly.
    """

    def __init__(self, **kwargs) -> None:
        """Constructor

        Keyword Arguments:
            **kwargs: Base class arguments.
        """
        super().__init__(**kwargs)

        self._join_refs = []

    def join(self, dict_or_table_factor: str | dict, join_cond: str | Sequence | None = None, join_type: str = "INNER") -> Stmt:
        """Join a table with a JOIN condition.

        Arguments:
            dict_or_table_factor (string or dict): Name of table to join or :py:class:`dict` mapping
                table names and conditions to join.
            join_cond (mixed, optional): A string, tuple or list of conditions:

                * string (not dot-prefixed):
                    ``join(table, 'Field1')`` becomes
                    ``JOIN table USING (Field1)``
                * dot-prefixed string:
                    ``join(table, '.Field1')`` becomes
                    ``JOIN table ON (root_table_alias.Field1 = table.Field1)``
                * dot-dot-prefixed string:
                    ``join(table, '..Field1')`` becomes
                    ``JOIN table ON (previous_join_table.Field1 = table.Field1)``
                * tuple with two dot-prefixed items:
                    ``join(table, ('.Field1', '.Field2'))`` becomes
                    ``JOIN table ON (root_table_alias.Field1 = table.Field2)``
                * tuple with two or more items:
                    ``join(table, ('Field1', 'Field2', ...))`` becomes
                    ``JOIN table USING (Field1, Field2, ...)``
                * list of conditions:
                    ``join(table, [condition,...])`` becomes
                    ``JOIN table ON (condition [AND condition [AND ...]])``

            join_type (string, optional): The type of join, such as ``INNER``, ``LEFT``,
                ``CROSS``, ``STRAIGHT_JOIN``, ``LEFT OUTER``, etc. The default is ``INNER``.

        Examples: ::

            >>> q = Select()
            >>> q.columns(['t1c1', 't2c1']).from_table('t1').join('t2', 't1c1', 'STRAIGHT_JOIN').sql()
            ('SELECT `t1c1`, `t2c1` FROM t1 STRAIGHT_JOIN t2 USING (`t1c1`)', None)
        """
        # Turn 'INNER' into 'INNER JOIN' but ignore 'STRAIGHT_JOIN'
        if "JOIN" not in join_type:
            join_type += " JOIN"

        if not isinstance(dict_or_table_factor, str):
            for table_factor, cond in dict_or_table_factor.items():
                self.join(table_factor, cond, join_type)
        else:
            self._join_refs.append((join_type, dict_or_table_factor, join_cond))

        return self

    def left_join(self, table_or_dict: str | dict, join_cond: str | Sequence | None = None) -> Stmt:
        """Convenience function to create a LEFT JOIN. See :py:meth:`join` for details.

        Examples: ::

            >>> q = Select()
            >>> q.columns(['t1c1', 't2c1']).from_table('t1').left_join('t2', 't1c1').sql()
            ('SELECT `t1c1`, `t2c1` FROM t1 LEFT JOIN t2 USING (`t1c1`)', None)

            >>> q = Select()
            >>> q.columns(['t1c1', 't2c1']).from_table('t1').left_join('t2', '.t1c1').sql()
            ('SELECT `t1c1`, `t2c1` FROM t1 LEFT JOIN t2 ON (t1.`t1c1` = t2.`t1c1`)', None)

            >>> q = Select()
            >>> q.columns(['t1c1', 't2c1']).from_table('t1').left_join('t2', ('t1c1','t2c1')).sql()
            ('SELECT `t1c1`, `t2c1` FROM t1 LEFT JOIN t2 USING (`t1c1`, `t2c1`)', None)

            >>> q = Select()
            >>> q.columns(['t1c1', 't2c1']).from_table('t1').left_join('t2', ('.t1c1','.t2c1')).sql()
            ('SELECT `t1c1`, `t2c1` FROM t1 LEFT JOIN t2 ON (t1.`t1c1` = t2.`t2c1`)', None)

            >>> q = Select()
            >>> q.columns(['t1c1', 't2c1']).from_table('t1').left_join('t2', ['t1c1 = t2c1']).sql()
            ('SELECT `t1c1`, `t2c1` FROM t1 LEFT JOIN t2 ON (t1c1 = t2c1)', None)

            >>> q = Select()
            >>> q.columns(['t1c1', 't2c1']).from_table('t1').left_join('t2', ['t1c1 = t2c1']).sql()
            ('SELECT `t1c1`, `t2c1` FROM t1 LEFT JOIN t2 ON (t1c1 = t2c1)', None)

            >>> q = Select()
            >>> q.columns(['t1.t1c1', 't2.t2c1']).from_table('t1').left_join('t2', ('.t1c1','.t2c1')).sql()
            ('SELECT t1.`t1c1`, t2.`t2c1` FROM t1 LEFT JOIN t2 ON (t1.`t1c1` = t2.`t2c1`)', None)

            >>> q = Select()
            >>> q.columns(['t1a.c1', 't2a.c1']).from_table('t1 AS t1a').left_join('t2 AS t2a', ('.t1c1','.t2c1')).sql()
            ('SELECT t1a.`c1`, t2a.`c1` FROM t1 AS t1a LEFT JOIN t2 AS t2a ON (t1a.`t1c1` = t2a.`t2c1`)', None)

            >>> q = Select()
            >>> q.columns(['t1a.c1', 't2a.c1']).from_table('t1 AS t1a').left_join('t2 AS t2a', '.t1c1').sql()
            ('SELECT t1a.`c1`, t2a.`c1` FROM t1 AS t1a LEFT JOIN t2 AS t2a ON (t1a.`t1c1` = t2a.`t1c1`)', None)

            >>> q = Select()
            >>> q.columns(['t1c1', 't2c1', 't3c1']).from_table('t1').left_join('t2', '.t1c1').left_join('t3', '.t1c1').sql()
            ('SELECT `t1c1`, `t2c1`, `t3c1` FROM t1 LEFT JOIN t2 ON (t1.`t1c1` = t2.`t1c1`) LEFT JOIN t3 ON (t1.`t1c1` = t3.`t1c1`)', None)

            >>> q = Select()
            >>> q.columns(['t1c1', 't2c1', 't3c1']).from_table('t1').left_join(OrderedDict([('t2', '.t1c1'),('t3','.t1c1')])).sql()
            ('SELECT `t1c1`, `t2c1`, `t3c1` FROM t1 LEFT JOIN t2 ON (t1.`t1c1` = t2.`t1c1`) LEFT JOIN t3 ON (t1.`t1c1` = t3.`t1c1`)', None)

            >>> q = Select()
            >>> q.columns(['t1c1', 't2c1', 't3c1']).from_table('t1').left_join('t2', '..t1c1').left_join('t3', '..t2c1').sql()
            ('SELECT `t1c1`, `t2c1`, `t3c1` FROM t1 LEFT JOIN t2 ON (t1.`t1c1` = t2.`t1c1`) LEFT JOIN t3 ON (t2.`t2c1` = t3.`t2c1`)', None)
        """
        return self.join(table_or_dict, join_cond, "LEFT")

    def _append_join_table_refs(self, root_table_factor: str, table_refs: list) -> None:
        """

        :param root_table_factor:
        :param table_refs:
        """
        root_table_alias = self.table_alias(root_table_factor)

        prev_join_table = root_table_alias

        for join_type, join_table_factor, join_cond in self._join_refs:
            join_table = self.table_alias(join_table_factor)

            format_args = {
                "join_table": join_table,
                "join_table_factor": join_table_factor,  # join_table [AS alias]
                "join_type": join_type,
                "root_table_alias": root_table_alias,
                "prev_join_table": prev_join_table,
            }

            if isinstance(join_cond, tuple) and join_cond[0].startswith("."):
                # tuple ('.Field1', '.Field2')
                # JOIN table ON (root_table_alias.Field1 = table.Field2)
                assert len(join_cond) == 2
                field1, field2 = join_cond
                join_clause = "{join_type} {join_table_factor} ON ({root_table_alias}.{field1} = {join_table}.{field2})".format(
                    field1=self.quote_col_ref(field1[1:]),
                    field2=self.quote_col_ref(field2[1:]),
                    **format_args,
                )

            elif isinstance(join_cond, tuple):
                # tuple ('Field1', 'Field2', ...)
                # JOIN table USING (Field1, Field2, ...)
                join_clause = "{join_type} {join_table_factor} USING ({column_list})".format(
                    column_list=", ".join([self.quote_col_ref(col) for col in join_cond]),
                    **format_args,
                )

            elif not isinstance(join_cond, str):
                # list [condition,...]
                # JOIN table ON (condition [AND condition [AND ...]])
                join_clause = "{join_type} {join_table_factor} ON ({conditions})".format(conditions=" AND ".join(join_cond), **format_args)

            elif join_cond.startswith(".."):
                # '..Field1'
                # JOIN table ON (prev_join_table.Field1 = table.Field1)
                join_clause = "{join_type} {join_table_factor} ON ({prev_join_table}.{field} = {join_table}.{field})".format(
                    field=self.quote_col_ref(join_cond[2:]),
                    **format_args,
                )

            elif join_cond.startswith("."):
                # '.Field1'
                # JOIN table ON (root_table_alias.Field1 = table.Field1)
                join_clause = "{join_type} {join_table_factor} ON ({root_table_alias}.{field} = {join_table}.{field})".format(
                    field=self.quote_col_ref(join_cond[1:]),
                    **format_args,
                )

            else:
                # 'Field1'
                # JOIN table USING (Field1)
                join_clause = "{join_type} {join_table_factor} USING ({field})".format(field=self.quote_col_ref(join_cond), **format_args)

            table_refs.append(join_clause)

            prev_join_table = join_table
