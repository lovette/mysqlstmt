"""mysqlstmt delete class module.

This module provides:
- Delete
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .join_mixin import JoinMixin
from .stmt import Stmt
from .where_mixin import WhereMixin

if TYPE_CHECKING:
    from collections.abc import Sequence


class Delete(Stmt, WhereMixin, JoinMixin):
    """DELETE statement.

    Examples: ::

        >>> q = Delete('t1')
        >>> q.where_value('t1c1', 1).sql()
        ('DELETE FROM t1 WHERE `t1c1` = 1', None)

        >>> q = Delete()
        >>> q.from_table('t1').where_value('t1c1', 1).sql()
        ('DELETE FROM t1 WHERE `t1c1` = 1', None)

        >>> q = Delete()
        >>> q.from_table('t1').where_value('t1c1', 1)()
        ('DELETE FROM t1 WHERE `t1c1` = 1', None)

        >>> q = Delete()
        >>> q.from_table('t1').where_value('t1c1', 1).where_value('t1c2', 2).sql()
        ('DELETE FROM t1 WHERE (`t1c1` = 1 AND `t1c2` = 2)', None)

        >>> q = Delete()
        >>> values = {'t1c1': 1}
        >>> q.from_table('t1').where_value(values).sql()
        ('DELETE FROM t1 WHERE `t1c1` = 1', None)

        >>> q = Delete()
        >>> values = OrderedDict([('t1c1',1), ('t1c2',2)])
        >>> q.from_table('t1').where_value(values).sql()
        ('DELETE FROM t1 WHERE (`t1c1` = 1 AND `t1c2` = 2)', None)

        >>> q = Delete()
        >>> values = OrderedDict([('t1c1','a'), ('t1c2','b')])
        >>> q.from_table('t1').where_value(values).sql()
        ("DELETE FROM t1 WHERE (`t1c1` = ? AND `t1c2` = ?)", ['a','b'])

        >>> q = Delete()
        >>> values = OrderedDict([('t1c1','a'), ('t1c2',None)])
        >>> q.from_table('t1').where_value(values).sql()
        ("DELETE FROM t1 WHERE (`t1c1` = ? AND `t1c2` IS NULL)", ['a'])

        >>> q = Delete()
        >>> q.from_table('t1').where_value('t1c1', 'NOW()').sql()
        ('DELETE FROM t1 WHERE `t1c1` = ?', ['NOW()'])

        >>> q = Delete()
        >>> q.from_table('t1').where_raw_value('t1c1', 't1c1+1').sql()
        ('DELETE FROM t1 WHERE `t1c1` = t1c1+1', None)

        >>> q = Delete()
        >>> q.from_table('t1').where_raw_value({'t1c1':'NOW()'}).sql()
        ('DELETE FROM t1 WHERE `t1c1` = NOW()', None)

        >>> q = Delete()
        >>> q.from_table('t1').where_value('t1c1', 1).order_by('t1c2').sql()
        ('DELETE FROM t1 WHERE `t1c1` = 1 ORDER BY t1c2', None)

        >>> q = Delete()
        >>> q.from_table('t1').where_value('t1c1', 1).order_by(['t1c1','t1c2']).sql()
        ('DELETE FROM t1 WHERE `t1c1` = 1 ORDER BY t1c1, t1c2', None)

        >>> q = Delete()
        >>> q.from_table('t1').where_value('t1c1', 1).limit(5).sql()
        ('DELETE FROM t1 WHERE `t1c1` = 1 LIMIT 5', None)

        >>> q = Delete(ignore_error=True)
        >>> q.from_table('t1').where_value('t1c1', 1).sql()
        ('DELETE IGNORE FROM t1 WHERE `t1c1` = 1', None)

        >>> q = Delete(allow_unqualified_delete=True)
        >>> q.from_table('t1').sql()
        ('DELETE FROM t1', None)

        >>> q = Delete(placeholder=False)
        >>> q.from_table('t1').where_value('t1c1', 1).limit(5).sql()
        'DELETE FROM t1 WHERE `t1c1` = 1 LIMIT 5'

        >>> q = Delete('t1')
        >>> q.join('t2', 't2c1').where_value('t2.t2c2', 1).where_value('t2.t2c3', 2).sql()
        ('DELETE FROM t1 USING t1 INNER JOIN t2 USING (`t2c1`) WHERE (t2.`t2c2` = 1 AND t2.`t2c3` = 2)', None)

        >>> q = Delete(('t1', 't2'), allow_unqualified_delete=True)
        >>> q.join('t2', '.t1c1').join('t3', '..t1c1').sql()
        ('DELETE FROM t1, t2 USING t1 INNER JOIN t2 ON (t1.`t1c1` = t2.`t1c1`) INNER JOIN t3 ON (t2.`t1c1` = t3.`t1c1`)', None)

        >>> q = Delete()
        >>> q.set_option('LOW_PRIORITY').from_table('t1').where_value('t1c1', 1).sql()
        ('DELETE LOW_PRIORITY FROM t1 WHERE `t1c1` = 1', None)
    """

    def __init__(self, table_name: str | None = None, ignore_error: bool = False, allow_unqualified_delete: bool = False, **kwargs) -> None:
        """Constructor

        Keyword Arguments:
            table_name (string or list, optional): Table(s) to delete from.
            ignore_error (bool, optional): Include IGNORE flag in statement.
            where_predicate (string, optional): The predicate for the outer WHERE condition, either 'AND' or 'OR'.
            **kwargs: Base class arguments.
        """
        super().__init__(**kwargs)

        # Public flags
        self.ignore_error = ignore_error
        self.allow_unqualified_delete = allow_unqualified_delete

        # Internals
        self._table_names = []
        self._orderby_conds = []
        self._limit = None

        if table_name:
            self.from_table(table_name)

    def from_table(self, list_or_name: str | Sequence) -> Delete:
        """Add table(s) to delete from.

        Arguments:
            list_or_name (string or list): Table name or list of table names.

        Returns:
            object: self

        Raises:
            ValueError: More than one table was specified.
        """
        if not isinstance(list_or_name, str):
            self._table_names.extend(list_or_name)
        else:
            self._table_names.append(list_or_name)

        return self

    def order_by(self, list_or_name: str | Sequence) -> Delete:
        """Add expressions to order by.

        Arguments:
            list_or_name (string or list): An expression or list of expressions to order by.

        Returns:
            object: self
        """
        if not isinstance(list_or_name, str):
            for c in list_or_name:
                self.order_by(c)
        else:
            self._orderby_conds.append(list_or_name)

        return self

    def limit(self, row_count: int) -> Delete:
        """Add limit clause expression.

        Arguments:
            row_count (int): Maximum number of rows to return.

        Returns:
            object: self
        """
        self._limit = row_count
        return self

    def sql(self) -> str:
        """Build DELETE SQL statement.

        Returns:
            Either a tuple ``(SQL statement, parameterized values)`` if ``placeholder`` is set,
            otherwise SQL statement as string.

        Raises:
            ValueError: The statement cannot be created with the given attributes.
        """
        if not self._table_names:
            msg = "DELETE requires at least one table"
            raise ValueError(msg)

        param_values = []
        multi_table = len(self._table_names) > 1 or len(self._join_refs) > 0

        # MySQL DELETE syntax as of 5.7:
        #
        # Single-table:
        #
        # > DELETE [LOW_PRIORITY] [QUICK] [IGNORE] FROM tbl_name
        # >     [PARTITION (partition_name,...)]
        # >     [WHERE where_condition]
        # >     [ORDER BY ...]
        # >     [LIMIT row_count]
        #
        # Multiple-table:
        #
        # > DELETE [LOW_PRIORITY] [QUICK] [IGNORE]
        # >     FROM tbl_name[.*] [, tbl_name[.*]] ...
        # >     USING table_references
        # >     [WHERE where_condition]

        sql = ["DELETE"]

        if self.query_options:
            sql.extend(self.query_options)

        if self.ignore_error:
            sql.append("IGNORE")

        if not multi_table:
            sql.append("FROM")
            sql.append(self._table_names[0])

            if self._where_cond_root.has_conds:
                sql.append("WHERE")
                sql.append(self._where_cond_root.sql(param_values))
            elif not self.allow_unqualified_delete:
                msg = "DANGER! Unqualified deletes can ruin your day!"
                raise ValueError(msg)

            if self._orderby_conds:
                sql.append("ORDER BY")
                sql.append(", ".join(self._orderby_conds))

            if self._limit:
                sql.append(f"LIMIT {self._limit}")
        else:
            sql.append("FROM")
            sql.append(", ".join(self._table_names))

            if self._join_refs:
                sql.append("USING")
                table_refs = [self._table_names[0]]
                self._append_join_table_refs(self._table_names[0], table_refs)
                sql.append(" ".join(table_refs))

            if self._where_cond_root.has_conds:
                sql.append("WHERE")
                sql.append(self._where_cond_root.sql(param_values))
            elif not self.allow_unqualified_delete:
                msg = "DANGER! Unqualified deletes can ruin your day!"
                raise ValueError(msg)

            if self._orderby_conds:
                msg = "ORDER BY not supported when DELETE FROM multiple tables"
                raise ValueError(msg)
            if self._limit:
                msg = "LIMIT not supported when DELETE FROM multiple tables"
                raise ValueError(msg)

        if self.placeholder:
            return " ".join(sql), param_values if param_values else None
        assert not param_values
        return " ".join(sql)
