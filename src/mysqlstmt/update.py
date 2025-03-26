"""mysqlstmt update class module.

This module provides:
- Update
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .join_mixin import JoinMixin
from .set_values_mixin import SetValuesMixin
from .stmt import Stmt
from .where_mixin import WhereMixin

if TYPE_CHECKING:
    from collections.abc import Sequence

    from .stmt import SQLPReturnT


class Update(Stmt, WhereMixin, JoinMixin, SetValuesMixin):
    """UPDATE statement.

    Examples: ::

        >>> q = Update('t1')
        >>> q.set_value('t1c1', 1).sql()
        ('UPDATE t1 SET `t1c1`=1', None)

        >>> q = Update()
        >>> q.table('t1').set_value('t1c1', 1).sql()
        ('UPDATE t1 SET `t1c1`=1', None)

        >>> q = Update()
        >>> q.table('t1').set_value('t1c1', 1)()
        ('UPDATE t1 SET `t1c1`=1', None)

        >>> q = Update()
        >>> q.table('t1').set_value('t1c1', 1).set_value('t1c2', 2).sql()
        ('UPDATE t1 SET `t1c1`=1, `t1c2`=2', None)

        >>> q = Update()
        >>> values = {'t1c1': 1}
        >>> q.table('t1').set_value(values).sql()
        ('UPDATE t1 SET `t1c1`=1', None)

        >>> q = Update()
        >>> values = OrderedDict([('t1c1',1), ('t1c2',2)])
        >>> q.table('t1').set_value(values).sql()
        ('UPDATE t1 SET `t1c1`=1, `t1c2`=2', None)

        >>> q = Update()
        >>> values = OrderedDict([('t1c1','a'), ('t1c2','b')])
        >>> q.table('t1').set_value(values).sql()
        ("UPDATE t1 SET `t1c1`=?, `t1c2`=?", ['a','b'])

        >>> q = Update()
        >>> values = OrderedDict([('t1c1','a'), ('t1c2',None)])
        >>> q.table('t1').set_value(values).sql()
        ("UPDATE t1 SET `t1c1`=?, `t1c2`=NULL", ['a'])

        >>> q = Update()
        >>> q.table('t1').set_value('t1c1', 'NOW()').sql()
        ('UPDATE t1 SET `t1c1`=?', ['NOW()'])

        >>> q = Update()
        >>> q.table('t1').set_raw_value('t1c1', 't1c1+1').sql()
        ('UPDATE t1 SET `t1c1`=t1c1+1', None)

        >>> q = Update()
        >>> q.table('t1').set_raw_value({'t1c1':'NOW()'}).sql()
        ('UPDATE t1 SET `t1c1`=NOW()', None)

        >>> q = Update()
        >>> q.table('t1').set_value('t1c1', 1).order_by('t1c2').sql()
        ('UPDATE t1 SET `t1c1`=1 ORDER BY t1c2', None)

        >>> q = Update()
        >>> q.table('t1').set_value('t1c1', 1).order_by(['t1c1','t1c2']).sql()
        ('UPDATE t1 SET `t1c1`=1 ORDER BY t1c1, t1c2', None)

        >>> q = Update()
        >>> q.table('t1').set_value('t1c1', 1).limit(5).sql()
        ('UPDATE t1 SET `t1c1`=1 LIMIT 5', None)

        >>> q = Update(ignore_error=True)
        >>> q.table('t1').set_value('t1c1', 1).sql()
        ('UPDATE IGNORE t1 SET `t1c1`=1', None)

        >>> q = Update()
        >>> q.table('t1').set_value('t1c1', 1).where_value('t1c2', 5).sql()
        ('UPDATE t1 SET `t1c1`=1 WHERE `t1c2` = 5', None)

        >>> q = Update()
        >>> q.table('t1').set_value('t1c1', 1).join('t2', 't1c1').where_value('t2c1', None).sql()
        ('UPDATE t1 INNER JOIN t2 USING (`t1c1`) SET `t1c1`=1 WHERE `t2c1` IS NULL', None)

        >>> q = Update()
        >>> q.table(['t1','t2']).set_value('t1c1', 1).where_expr('(`t1c1` = `t2c1`)').sql()
        ('UPDATE t1, t2 SET `t1c1`=1 WHERE (`t1c1` = `t2c1`)', None)

        >>> q = Update(placeholder=False)
        >>> q.table('t1').set_value('t1c1', 1).where_value('t1c2', 5).sql()
        UPDATE t1 SET `t1c1`=1 WHERE `t1c2` = 5

        >>> q = Update()
        >>> q.set_option('LOW_PRIORITY').table('t1').set_value('t1c1', 1).sql()
        ('UPDATE LOW_PRIORITY t1 SET `t1c1`=1', None)
    """

    def __init__(self, table_name: str | None = None, ignore_error: bool = False, **kwargs) -> None:
        """Constructor.

        Keyword Arguments:
            table_name (string, optional): Table or tables to update.
            ignore_error (bool, optional): Include IGNORE flag in statement.
            where_predicate (string, optional): The predicate for the outer WHERE condition, either 'AND' or 'OR'.
            **kwargs: Base class arguments.
        """
        super().__init__(**kwargs)

        # Public flags
        self.ignore_error = ignore_error

        # Internals
        self._table_names: list[str] = []
        self._orderby_conds: list[str] = []
        self._limit = None

        if table_name:
            self.table(table_name)

    def table(self, list_or_name: str | Sequence[str]) -> Update:
        """Add tables to update.

        Arguments:
            list_or_name (string or list): Table name or list of table names.

        Returns:
            object: self
        """
        if not isinstance(list_or_name, str):
            for c in list_or_name:
                self.table(c)
        else:
            self._table_names.append(list_or_name)

        return self

    from_tables = table
    """Alias for :py:meth:`from_table`"""

    def order_by(self, list_or_name: str | Sequence[str]) -> Update:
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

    def limit(self, row_count: int) -> Update:
        """Add limit clause expression.

        Arguments:
            row_count (int): Maximum number of rows to return.

        Returns:
            object: self
        """
        self._limit = row_count
        return self

    def sqlp(self) -> SQLPReturnT:  # noqa: C901, PLR0912
        """Build UPDATE SQL statement.

        Returns:
            Either a tuple ``(SQL statement, parameterized values)`` if ``placeholder`` is set,
            otherwise SQL statement as string.

        Raises:
            ValueError: The statement cannot be created with the given attributes.
        """
        if not self._table_names:
            msg = "UPDATE requires at least one table"
            raise ValueError(msg)
        if not self._values and not self._values_raw:
            msg = "UPDATE requires at least one value"
            raise ValueError(msg)

        table_refs = [", ".join(self._table_names)]
        param_values = []
        col_names = []
        inline_values = []
        set_values = []

        self._append_join_table_refs(self._table_names[0], table_refs)

        if self._values:
            for col, val in self._values.items():
                col_names.append(col)
                self.parameterize_values(val, inline_values, param_values)

        for col in self._values_raw:
            val, val_params = self._values_raw[col]
            col_names.append(col)
            inline_values.append(val)
            if val_params is not None and self.placeholder:
                param_values.extend(val_params)

        assert len(col_names) == len(inline_values)
        for col, val in zip(col_names, inline_values):
            set_values.append(f"{self.quote_col_ref(col)}={val}")

        # MySQL UPDATE syntax as of 5.7:
        #
        # Single-table syntax:
        #
        # UPDATE [LOW_PRIORITY] [IGNORE] table_reference
        #     SET col_name1={expr1|DEFAULT} [, col_name2={expr2|DEFAULT}] ...
        #     [WHERE where_condition]
        #     [ORDER BY ...]
        #     [LIMIT row_count]
        #
        # Multiple-table syntax:
        #
        # UPDATE [LOW_PRIORITY] [IGNORE] table_references
        #     SET col_name1={expr1|DEFAULT} [, col_name2={expr2|DEFAULT}] ...
        #     [WHERE where_condition]

        sql = ["UPDATE"]

        if self.query_options:
            sql.extend(self.query_options)

        if self.ignore_error:
            sql.append("IGNORE")

        sql.append(" ".join(table_refs))

        sql.append("SET")
        sql.append(", ".join(set_values))

        where_cond = self._where_cond_root.sql(param_values)
        if where_cond:
            sql.append("WHERE")
            sql.append(where_cond)

        if self._orderby_conds:
            if len(self._table_names) + len(self._join_refs) > 1:
                msg = "Multiple-table UPDATE does not support ORDER BY"
                raise ValueError(msg)

            sql.append("ORDER BY")
            sql.append(", ".join(self._orderby_conds))

        if self._limit:
            if len(self._table_names) + len(self._join_refs) > 1:
                msg = "Multiple-table UPDATE does not support LIMIT"
                raise ValueError(msg)

            sql.append(f"LIMIT {self._limit}")

        if self.placeholder:
            return " ".join(sql), param_values if param_values else None
        assert not param_values
        return " ".join(sql), None
