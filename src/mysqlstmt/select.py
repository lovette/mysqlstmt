"""mysqlstmt select class module.

This module provides:
- Select
"""  # noqa: A005

from __future__ import annotations

from collections.abc import Collection, Sequence
from typing import TYPE_CHECKING, NamedTuple
from typing import Union as UnionT

from .config import Config
from .join_mixin import JoinMixin
from .stmt import Stmt
from .where_condition import StmtParamValuesT, ValueParamsT, WhereCondition
from .where_mixin import WhereMixin

if TYPE_CHECKING:
    from collections.abc import Mapping

    from typing_extensions import Self

    from .stmt import SelectExprT, SQLReturnT
    from .where_condition import WhereExprT, WhereOpT, WherePredT, WhereRawValueT, WhereValueT


class SelectColumnT(NamedTuple):
    """Select column tuple.

    Attributes:
        expr (str): Column name or expression.
        named (str):  Column alias or None.
        value_params (ValueParamsT): Value params or None.
        quote (bool): True if expression is to be quoted.
        raw (bool): True if `expr` is an expression.
    """

    expr: str
    named: UnionT[str, None]  # noqa: UP007
    value_params: UnionT[ValueParamsT, None]  # noqa: UP007
    quote: bool
    raw: bool


TableFactorT = tuple[
    UnionT[str, Stmt],  # table | mysqlstmt.Select
    UnionT[str, None],  # named
]

FromTableT = UnionT[str, Stmt, Sequence[str]]


class Select(Stmt, WhereMixin, JoinMixin):
    """SELECT statement.

    Attributes:
        cacheable (bool): See constructor.
        calc_found_rows (bool): See constructor.

    Examples: ::

        >>> q = Select('t1')
        >>> q.columns('t1c1').sql()
        ('SELECT `t1c1` FROM t1', None)

        >>> q = Select()
        >>> q.from_table('t1').sql()
        ('SELECT * FROM t1', None)

        >>> q = Select(cacheable=True)
        >>> sql_t = q.from_table('t1').columns('t1c1').sql()
        ('SELECT SQL_CACHE `t1c1` FROM t1', None)

        >>> q = Select(cacheable=False)
        >>> sql_t = q.from_table('t1').columns('t1c1').sql()
        ('SELECT SQL_NO_CACHE `t1c1` FROM t1', None)

        >>> q = Select('t1')
        >>> q.set_option('DISTINCT').columns('t1c1').sql()
        ('SELECT DISTINCT `t1c1` FROM t1', None)
    """

    def __init__(  # noqa: PLR0913
        self,
        table_name: FromTableT | None = None,
        having_predicate: str = "OR",
        cacheable: bool | None = None,
        calc_found_rows: bool = False,
        distinct: bool = False,
        named: str | None = None,
        **kwargs,
    ) -> None:
        """Constructor.

        Keyword Arguments:
            table_name (string | list[string] | Stmt, optional): Table, tables or SELECT to select from.
            having_predicate (string, optional): The predicate for the outer HAVING condition, either 'AND' or 'OR'.
                Default is 'OR'.
            where_predicate (string, optional): The predicate for the outer WHERE condition, either 'AND' or 'OR'.
                Default is 'OR'.
            cacheable (bool, optional): Whether MySQL should cache query result.
                Default is None, in which case the :py:class:`mysqlstmt.config.Config` setting will be used.
            calc_found_rows (bool, optional): Whether MySQL should calculate number of found rows. Default is False.
            distinct (bool, optional): Whether to use DISTINCT. Default is False.
            named (str, optional): Name table using "AS NAME". Default is None.
            **kwargs: Base class arguments.
        """
        super().__init__(**kwargs)

        assert having_predicate in ("AND", "OR")

        self._table_factors: list[TableFactorT] = []
        self._select_col: list[SelectColumnT] = []
        self._orderby_conds = []
        self._groupby_conds = []
        self._limit = None
        self._having_cond_root = WhereCondition(self, where_predicate=having_predicate)

        self.cacheable = Config.select_cacheable if cacheable is None else cacheable

        self.calc_found_rows = calc_found_rows

        # Default first condition is 'AND'; will be ignored if having_or is called first
        self.having_cond(where_predicate="AND")

        if table_name:
            self.from_table(table_name, named=named)
        if distinct:
            self.set_option("DISTINCT")

    def from_table(self, list_or_name: FromTableT, named: str | None = None) -> Select:
        """Add tables to select from.

        Arguments:
            list_or_name (string or list): Table name or list of table names.
            named (str, optional): Name table using "AS NAME". Default is None.

        Returns:
            object: self

        Examples: ::

            >>> q = Select('t1')
            >>> q.sql()
            ('SELECT * FROM t1', None)

            >>> q = Select()
            >>> q.from_table('t1').sql()
            ('SELECT * FROM t1', None)
        """
        if not isinstance(list_or_name, (Stmt, str)):
            if named is not None:
                msg = "Tables cannot be named when using a list of tables"
                raise ValueError(msg)

            for c in list_or_name:
                self.from_table(c)
        else:
            self._table_factors.append((list_or_name, named))

        return self

    def from_select(self, qselect: Select, named: str | None = None) -> Select:
        """Add subquery SELECT to select from.

        Arguments:
            qselect (Select): Select query.
            named (str, optional): Name table using "AS NAME". Default is None.

        Returns:
            object: self

        Examples: ::

            >>> q = Select().from_select(Select('t2').column('t2c1'))
            >>> q.sql()
            ('SELECT * FROM (SELECT `t2c1` FROM t2)', None)
        """
        self.from_table(qselect, named=named)

        return self

    from_tables = from_table
    """Alias for :py:meth:`from_table`"""

    def column(
        self,
        list_or_name: SelectExprT,
        raw: bool = False,
        value_params: ValueParamsT | None = None,
        named: str | None = None,
        quote: bool = False,
    ) -> Select:
        """Add column names to select.

        Arguments:
            list_or_name (string or list): Column name or list of column names.
            raw (bool, optional): Set to True for column name to be included in the SQL verbatim, default is False.
            value_params (Sequence, optional): List of value params if ``raw`` is True. Default is None.
            named (str, optional): Name column using "AS NAME". Default is None.
            quote (bool, optional): Quote the expression if necessary. Default is False. Applies only if ``raw`` is True.

        Returns:
            object: self

        Examples: ::

            >>> q = Select()
            >>> q.from_table('t1').columns('t1c1').sql()
            ('SELECT `t1c1` FROM t1', None)

            >>> q = Select()
            >>> q.from_table('t1').columns('t1.t1c1').sql()
            ('SELECT t1.`t1c1` FROM t1', None)

            >>> q = Select(quote_all_col_refs = False)
            >>> q.from_table('t1').columns('t1.t1c1').sql()
            ('SELECT t1.t1c1 FROM t1', None)

            >>> q = Select()
            >>> q.from_table('t1 AS t1a').columns('t1c1').sql()
            ('SELECT `t1c1` FROM t1 AS t1a', None)

            >>> q = Select()
            >>> q.from_table('t1').columns('t1c1')()
            ('SELECT `t1c1` FROM t1', None)

            >>> q = Select()
            >>> q.from_table('t1').column('t1c1').column('t1c2').sql()
            ('SELECT `t1c1`, `t1c2` FROM t1', None)

            >>> q = Select()
            >>> q.from_table('t1').columns(['t1c1','t1c2']).sql()
            ('SELECT `t1c1`, `t1c2` FROM t1', None)

            >>> q = Select()
            >>> q.from_table('t1').columns(('t1c1','t1c2')).sql()
            ('SELECT `t1c1`, `t1c2` FROM t1', None)

            >>> q = Select()
            >>> q.from_table('t1').columns('`t1c1`').sql()
            ('SELECT `t1c1` FROM t1', None)

            >>> q = Select()
            >>> q.from_table('t1').columns('DATE(`t1c1`)').sql()
            ('SELECT DATE(`t1c1`) FROM t1', None)

            >>> q = Select()
            >>> q.from_table('t1').columns('`t1c1` AS `t1a1`').sql()
            ('SELECT `t1c1` AS `t1a1` FROM t1', None)
        """
        assert value_params is None or isinstance(value_params, Sequence)

        if not isinstance(list_or_name, str):
            for c in list_or_name:
                self.column(c, raw, value_params, None, quote)
        else:
            if named is None:
                name_parts = list_or_name.split(" AS ", 1)
                if len(name_parts) == 2:  # noqa: PLR2004
                    list_or_name, named = name_parts

            if self.is_selected(named or list_or_name):
                msg = f"Column '{named or list_or_name}' already exists"
                raise ValueError(msg)

            if not raw:
                value_params = None
                quote = False

            self._select_col.append(SelectColumnT(list_or_name, named, value_params, quote, raw))

        return self

    select = column
    """Alias for :py:meth:`column`"""

    columns = column
    """Alias for :py:meth:`column`"""

    def column_expr(
        self,
        list_or_expr: SelectExprT,
        value_params: ValueParamsT | None = None,
        named: str | None = None,
        quote: bool = False,
    ) -> Select:
        """Add expressions to select.

        Arguments:
            list_or_expr (string or list): Expression or list of expressions.
            value_params (Sequence, optional): List of value params. Default is None.
            named (str, optional): Name result using "AS NAME". Default is None.
            quote (bool, optional): True to quote the value if necessary. Default is False.

        Returns:
            object: self

        Examples: ::

            >>> q = Select()
            >>> q.column_expr('1+1').sql()
            ('SELECT 1+1', None)

            >>> q = Select()
            >>> q.column_expr('PASSWORD(?)', ['mypw']).sql()
            ('SELECT PASSWORD(?)', ['mypw'])
        """
        return self.column(list_or_expr, raw=True, value_params=value_params, named=named, quote=quote)

    select_expr = column_expr
    """Alias for :py:meth:`column_expr`"""

    columns_expr = column_expr
    """Alias for :py:meth:`column_expr`"""

    def is_selected(self, col_name: str) -> bool:
        """Check if a column name or alias is selected.

        Arguments:
            col_name (string): Column name.

        Returns:
            bool

        Examples: ::

            >>> q = Select()
            >>> q.from_table('t1').columns('t1c1')
            >>> q.is_select_col('t1c1')
            True
        """
        return self.get_column(col_name) is not None

    def get_column(self, col_name: str) -> SelectColumnT | None:
        """Get details of named column.

        Will find quoted and unquoted column names.
        Will not find table qualified column names.

        Arguments:
            col_name (string): Column name.

        Returns:
            SelectColumnT or None if column is not selected.

        Examples: ::

            >>> q = Select()
            >>> q.from_table('t1').columns('t1c1')
            >>> q.get_column('t1c1')
            ('t1c1', None, False, None)
        """
        for col_named in {col_name, self.quote_col_ref(col_name)}:
            for c in self._select_col:
                if c.expr == col_named or (c.named and c.named == col_named):
                    return c

        return None

    def remove_column(self, list_or_name: str | Collection[str] | SelectColumnT) -> Select:
        """Remove column names to select.

        Arguments:
            list_or_name (string or list): Column name or list of column names.

        Returns:
            object: self

        Examples: ::

            >>> q = Select()
            >>> q.from_table('t1').columns(('t1c1','t2c1')).remove_column('t2c1').sql()
            ('SELECT `t1c1` FROM t1', None)

            >>> q = Select()
            >>> q.from_table('t1').columns('t1c1').column_expr('1+1 AS t2c1').remove_column('t2c1').sql()
            ('SELECT `t1c1` FROM t1', None)
        """
        if isinstance(list_or_name, SelectColumnT):
            self.remove_column(list_or_name.named or list_or_name.expr)
        elif isinstance(list_or_name, str):
            self._select_col = [c for c in self._select_col if not (c.expr == list_or_name or (c.named and c.named == list_or_name))]
        else:
            for c in list_or_name:
                self.remove_column(c)

        return self

    def qualify_columns(self, table_name: str, qualify_cols: Collection[str] | None = None) -> Select:
        """Qualify column names with a table name.

        Arguments:
            table_name (string): Table name
            qualify_cols (Collection, optional): Column names to qualify,
                or None to qualify all non-qualified columns. Default is None.

        Returns:
            object: self

        Note:
            Does not qualify expression columns.

        Examples: ::
            >>> q = Select()
            >>> q.from_table('t1').columns(('t1c1', 't1c2')).qualify_columns('t1', ('t1c1',)).sql()
            ('SELECT t1.`t1c1`, `t1c2` FROM t1', None)

            >>> q = Select()
            >>> q.from_table('t1').columns(('t1c1', 't1c2')).qualify_columns('t1').sql()
            ('SELECT t1.`t1c1`, t1.`t1c2` FROM t1', None)

            >>> q = Select()
            >>> q.from_table(('t1', 't2')).columns(('t1c1', 't2.t2c1')).qualify_columns('t1').sql()
            ('SELECT t1.`t1c1`, t2.`t2c1` FROM t1, t2', None)
        """
        for i, col in enumerate(self._select_col):
            if (qualify_cols is None or col[0] in qualify_cols) and "." not in col[0]:
                self._select_col[i] = SelectColumnT(f"{table_name}.{col[0]}", *col[1:])

        return self

    def order_by(self, list_or_name: str | Sequence[str]) -> Select:
        """Add expressions to order by.

        Arguments:
            list_or_name (string or list): An expression or list of expressions to order by.

        Returns:
            object: self

        Examples: ::

            >>> q = Select()
            >>> q.from_table('t1').order_by('t1c1').sql()
            ('SELECT * FROM t1 ORDER BY t1c1', None)
        """
        if not isinstance(list_or_name, str):
            for c in list_or_name:
                self.order_by(c)
        else:
            self._orderby_conds.append(list_or_name)

        return self

    def group_by(self, list_or_name: str | Sequence[str]) -> Select:
        """Add expressions to group by.

        Arguments:
            list_or_name (string or list): An expression or list of expressions to group by.

        Returns:
            object: self

        Examples: ::

            >>> q = Select()
            >>> q.from_table('t1').group_by('t1c1').sql()
            ('SELECT * FROM t1 GROUP BY t1c1', None)

            >>> q = Select()
            >>> q.from_table('t1').group_by(['t1c1', 't1c2']).sql()
            ('SELECT * FROM t1 GROUP BY t1c1, t1c2', None)
        """
        if not isinstance(list_or_name, str):
            for c in list_or_name:
                self.group_by(c)
        else:
            self._groupby_conds.append(list_or_name)

        return self

    def limit(self, row_count: int, offset: int = 0) -> Select:
        """Add limit clause expression.

        Arguments:
            row_count (int): Maximum number of rows to return.
            offset (int, optional): Offset of the first row to return.

        Returns:
            object: self

        Examples: ::

            >>> q = Select()
            >>> q.from_table('t1').limit(5).sql()
            ('SELECT * FROM t1 LIMIT 5', None)
        """
        self._limit = (row_count, offset)
        return self

    def distinct(self) -> Select:
        """Set DISTINCT option.

        Returns:
            object: self
        """
        self.set_option("DISTINCT")
        return self

    def having_value(
        self,
        field_or_dict: str | Mapping[str, WhereValueT],
        value: WhereValueT | None = None,
        operator: WhereOpT = "=",  # pyright: ignore[reportInvalidTypeVarUse]
    ) -> Select:
        """Compare field to a value.

        Field names may be escaped with backticks.
        Use :py:meth:`having_expr` if you want field names to be
        included in the SQL statement verbatim.

        Values will be pickled by :py:meth:`mysqlstmt.stmt.Stmt.pickle`.
        Use :py:meth:`having_raw_value` if you want values to be
        included in the SQL statement verbatim.

        Arguments:
            field_or_dict (string or list): Name of field/column or :py:class:`dict` mapping fields to values.
            value (mixed, optional): Value to compare with if ``field_or_dict`` is a field name.
                Type can be anything that :py:meth:`mysqlstmt.stmt.Stmt.pickle` can handle (Sequence, Object,etc.).
            operator (string, optional): Comparison operator, default is '='.

        Returns:
            object: self

        Examples: ::

            >>> q = Select()
            >>> q.from_table('t1').having_value('t1c1', 3).sql()
            ('SELECT * FROM t1 HAVING `t1c1` = 3', None)

            >>> q = Select()
            >>> q.from_table('t1').having_value('t1c1', 3).having_value('t1c2', 'string').sql()
            ('SELECT * FROM t1 HAVING (`t1c1` = 3 AND `t1c2` = ?)', ['string'])

            >>> q = Select()
            >>> q.from_table('t1').having_value(OrderedDict([('t1c1', 3), ('t1c2', 'string')])).sql()
            ('SELECT * FROM t1 HAVING (`t1c1` = 3 AND `t1c2` = ?)', ['string'])

            >>> q = Select()
            >>> q.from_table('t1').having_value('t1c1', 1).having_value('t1c2', 5).having_and().having_value('t1c1', 6).having_value('t1c2', 10).sql()
            ('SELECT * FROM t1 HAVING ((`t1c1` = 1 AND `t1c2` = 5) OR (`t1c1` = 6 AND `t1c2` = 10))', None)
        """
        self.get_having_cond().where_value(field_or_dict, value, operator)
        return self

    having_values = having_value
    """Alias for :py:meth:`having_value`"""

    def having_raw_value(
        self,
        field_or_dict: str | Mapping[str, WhereRawValueT],
        raw_value: WhereRawValueT | None = None,
        operator: WhereOpT = "=",
        value_params: StmtParamValuesT | None = None,
    ) -> Select:
        """Compare field to a an unmanipulated value.

        Field names may be escaped with backticks.
        Use :py:meth:`having_expr` if you want field names to be
        included in the SQL statement verbatim.

        Values will be included in the SQL statement verbatim.
        Use :py:meth:`having_value` if you want values to be pickled.

        Arguments:
            field_or_dict (string or list): Name of field/column or :py:class:`dict` mapping fields to values.
            raw_value (string, optional): Value to compare with if ``field_or_dict`` is a field name.
            operator (string, optional): Comparison operator, default is '='.
            value_params (Sequence, optional): List of value params. Default is None.

        Returns:
            object: self

        Examples: ::

            >>> q = Select()
            >>> q.from_table('t1').having_raw_value('t1c1', 'NOW()').sql()
            ('SELECT * FROM t1 HAVING `t1c1` = NOW()', None)

            >>> q = Select()
            >>> q.from_table('t1').having_raw_value('t1c1', 'PASSWORD(?)', value_params=('mypw',)).sql()
            ('SELECT * FROM t1 WHERE `t1c1` = PASSWORD(?)', ['mypw'])
        """
        self.get_having_cond().where_raw_value(field_or_dict, raw_value, operator, value_params)
        return self

    having_raw_values = having_raw_value
    """Alias for :py:meth:`having_raw_value`"""

    def having_expr(self, list_or_expr: WhereExprT, expr_params: ValueParamsT | None = None) -> Select:
        """Include a complex expression in conditional statement.

        Expressions will be included in the SQL statement verbatim.
        Use :py:meth:`having_value` or :py:meth:`having_raw_value` if you are
        doing basic field comparisons and/or want the value to be pickled.

        Arguments:
            list_or_expr (string or list): An expression or :py:class:`list` of expressions.
            expr_params (Sequence, optional): List of expression params. Default is None.

        Returns:
            object: self

        Examples: ::

            >>> q = Select()
            >>> q.from_table('t1').having_expr('`t1c1` = NOW()').sql()
            ('SELECT * FROM t1 HAVING `t1c1` = NOW()', None)

            >>> q = Select()
            >>> q.from_table('t1').having_expr('`t1c1` = PASSWORD(?)', ('mypw',)).sql()
            ('SELECT * FROM t1 HAVING `t1c1` = PASSWORD(?)', ['mypw'])
        """
        self.get_having_cond().where_expr(list_or_expr, expr_params)
        return self

    having_exprs = having_expr
    """Alias for :py:meth:`having_expr`"""

    def get_having_cond(self, index: int = -1) -> WhereCondition:
        """Returns a ``WhereCondition`` object from the list of conditions.

        Arguments:
            index (int): Index of condition, defaults to the active condition (-1).

        Returns:
            object: :py:class:`WhereCondition`

        Note:
            Conditions are typically created with ``having_and`` and ``having_or``,
            so you should not need to use this function often.

        See Also:
            :py:class:`mysqlstmt.where_condition.WhereCondition` :py:meth:`having_cond`
        """
        return self._having_cond_root.get_where_cond(index)

    def having_cond(self, cond: WhereCondition | None = None, where_predicate: WherePredT | None = None) -> Self:
        """Activates a new ``WhereCondition``.

        Arguments:
            cond (mysqlstmt.WhereCondition, optional): A new condition; one will be created if not specified.
            where_predicate (string): The predicate for the new condition if a new one is created, either 'AND' or 'OR'.

        Returns:
            object: self

        Note:
            Conditions are typically created with ``having_and`` and ``having_or``.
            You should use this function when creating complex conditions with ``WhereCondition`` objects.

        See Also:
            :py:class:`mysqlstmt.where_condition.WhereCondition` :py:meth:`having_and` :py:meth:`having_or`
        """
        self._having_cond_root.add_cond(cond, where_predicate)
        return self

    def having_and(self) -> Select:
        """Activates a new ``WhereCondition`` with an 'AND' predicate.

        Returns:
            object: self

        See Also:
            :py:class:`mysqlstmt.where_condition.WhereCondition` :py:meth:`having_cond` :py:meth:`having_or`

        Examples: ::

            >>> q = Select()
            >>> q.from_table('t1').having_and().having_value('t1c1', 1).having_value('t1c2', 5). \
                having_and().having_value('t1c1', 6).having_value('t1c2', 10).sql()
            ('SELECT * FROM t1 HAVING ((`t1c1` = 1 AND `t1c2` = 5) OR (`t1c1` = 6 AND `t1c2` = 10))', None)
        """
        self._having_cond_root.where_and()
        return self

    def having_or(self) -> Select:
        """Activates a new ``WhereCondition`` with an 'OR' predicate.

        Returns:
            object: self

        See Also:
            :py:class:`mysqlstmt.where_condition.WhereCondition` :py:meth:`having_cond` :py:meth:`having_and`

        Examples: ::

            >>> q = Select()
            >>> q.from_table('t1').having_or().having_value('t1c1', 3).having_value('t1c1', 5).sql()
            ('SELECT * FROM t1 HAVING (`t1c1` = 3 OR `t1c1` = 5)', None)

            >>> q = Select(having_predicate='AND')
            >>> q.from_table('t1').having_or().having_value('t1c1', 1).having_value('t1c1', 5). \
                having_or().having_value('t1c1', 6).having_value('t1c1', 10).sql()
            ('SELECT * FROM t1 HAVING ((`t1c1` = 1 OR `t1c1` = 5) AND (`t1c1` = 6 OR `t1c1` = 10))', None)
        """
        self._having_cond_root.where_or()
        return self

    def sql(self) -> SQLReturnT:  # noqa: C901, PLR0912, PLR0915
        """Build SELECT SQL statement.

        Returns:
            Either a tuple ``(SQL statement, parameterized values)`` if ``placeholder`` is set,
            otherwise SQL statement as string.

        Raises:
            ValueError: The statement cannot be created with the given attributes.
        """

        def _named(self, table_or_select: str, named: str | None) -> str:  # noqa: ANN001
            if named:
                return " ".join([table_or_select, "AS", self.quote_col_ref(named)])
            return table_or_select

        table_refs = []
        param_values = []
        cols = []

        for c in self._select_col:
            expr, named, val_params, quote, raw = c

            if not raw:
                expr = self.quote_col_ref(expr)
            elif quote and isinstance(expr, str):
                expr = self.quote(expr)

            if named:
                expr = " ".join([expr, "AS", self.quote_col_ref(named)])

            cols.append(expr)

            if val_params is not None and self.placeholder:
                param_values.extend(val_params)

        if self._table_factors:
            _table_factors: list[str] = []

            for table_or_select, table_named in self._table_factors:
                if isinstance(table_or_select, Stmt):
                    # SELECT ... FROM (SELECT ...)
                    stmt_sql, stmt_params = table_or_select.sql() if self.placeholder else (table_or_select.sql(), None)
                    _table_factors.append(_named(self, f"({stmt_sql})", table_named))
                    if stmt_params:
                        param_values.extend(stmt_params)
                else:
                    _table_factors.append(_named(self, table_or_select, table_named))

            table_refs.append(", ".join(_table_factors))

        if self._join_refs:
            if not table_refs:
                msg = "A root table must be specified when using joins"
                raise ValueError(msg)

            root_table, root_named = self._table_factors[0]

            if not isinstance(root_table, str):
                msg = "Root table must be a string when using joins"
                raise ValueError(msg)

            self._append_join_table_refs(root_table if root_named is None else root_named, table_refs)

        # MySQL SELECT syntax as of 5.7:
        #
        # > SELECT
        # >     [ALL | DISTINCT | DISTINCTROW ]
        # >       [HIGH_PRIORITY]
        # >       [STRAIGHT_JOIN]
        # >       [SQL_SMALL_RESULT] [SQL_BIG_RESULT] [SQL_BUFFER_RESULT]
        # >       [SQL_CACHE | SQL_NO_CACHE] [SQL_CALC_FOUND_ROWS]
        # >     select_expr [, select_expr ...]
        # >     [FROM table_references
        # >     [WHERE where_condition]
        # >     [GROUP BY {col_name | expr | position}
        # >       [ASC | DESC], ... [WITH ROLLUP]]
        # >     [HAVING where_condition]
        # >     [ORDER BY {col_name | expr | position}
        # >       [ASC | DESC], ...]
        # >     [LIMIT {[offset,] row_count | row_count OFFSET offset}]
        # >     [PROCEDURE procedure_name(argument_list)]
        # >     [INTO OUTFILE 'file_name' export_options
        # >       | INTO DUMPFILE 'file_name'
        # >       | INTO var_name [, var_name]]
        # >     [FOR UPDATE | LOCK IN SHARE MODE]]

        sql = ["SELECT"]

        if self.query_options:
            sql.extend(self.query_options)

        if self.cacheable is True:
            sql.append("SQL_CACHE")
        elif self.cacheable is False:
            sql.append("SQL_NO_CACHE")

        if self.calc_found_rows is True:
            sql.append("SQL_CALC_FOUND_ROWS")

        sql.append(", ".join(cols) if cols else "*")

        if table_refs:
            sql.append("FROM")
            sql.append(" ".join(table_refs))

        where_cond = self._where_cond_root.sql(param_values)
        if where_cond:
            sql.append("WHERE")
            sql.append(where_cond)

        if self._groupby_conds:
            sql.append("GROUP BY")
            sql.append(", ".join(self._groupby_conds))

        having_cond = self._having_cond_root.sql(param_values)
        if having_cond:
            sql.append("HAVING")
            sql.append(having_cond)

        if self._orderby_conds:
            sql.append("ORDER BY")
            sql.append(", ".join(self._orderby_conds))

        if self._limit is not None:
            row_count, offset = self._limit
            if offset > 0:
                sql.append(f"LIMIT {offset},{row_count}")
            else:
                sql.append(f"LIMIT {row_count}")

        if self.placeholder:
            return " ".join(sql), param_values if param_values else None
        assert not param_values
        return " ".join(sql)
