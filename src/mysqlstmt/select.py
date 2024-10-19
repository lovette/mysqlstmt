import mysqlstmt
from .where_mixin import WhereMixin
from .join_mixin import JoinMixin
from .config import Config
import collections


class Select(mysqlstmt.Stmt, WhereMixin, JoinMixin):
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

    def __init__(self, table_name=None, having_predicate='OR', cacheable=None, calc_found_rows=False, **kwargs):
        """Constructor

        Keyword Arguments:
            table_name (string, optional): Table or tables to select from.
            having_predicate (string, optional): The predicate for the outer HAVING condition, either 'AND' or 'OR'.
            where_predicate (string, optional): The predicate for the outer WHERE condition, either 'AND' or 'OR'.
            cacheable (bool, optional): Whether MySQL should cache query result.
                Default is None, in which case the :py:class:`mysqlstmt.config.Config` setting will be used.
            calc_found_rows (bool, optional): Whether MySQL should calculate number of found rows. Default is False.
            **kwargs: Base class arguments.
        """
        super().__init__(**kwargs)

        assert having_predicate == 'AND' or having_predicate == 'OR'

        self._table_factors = []
        self._select_col = []
        self._select_expr = []  # tuple(name, value_params)
        self._orderby_conds = []
        self._groupby_conds = []
        self._limit = None
        self._having_cond_root = mysqlstmt.WhereCondition(self, where_predicate=having_predicate)

        if cacheable is False or Config.select_cacheable is False:
            self.cacheable = False
        else:
            self.cacheable = Config.select_cacheable if cacheable is None else cacheable

        self.calc_found_rows = calc_found_rows

        # Default first condition is 'AND'; will be ignored if having_or is called first
        self.having_cond(where_predicate='AND')

        if table_name:
            self.from_table(table_name)

    def from_table(self, list_or_name):
        """Add tables to select from.

        Arguments:
            list_or_name (string or list): Table name or list of table names.

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
        if not isinstance(list_or_name, basestring):
            for c in list_or_name:
                self.from_table(c)
        else:
            self._table_factors.append(list_or_name)

        return self

    from_tables = from_table
    """Alias for :py:meth:`from_table`"""

    def column(self, list_or_name, raw=False, value_params=None):
        """Add column names to select.

        Arguments:
            list_or_name (string or list): Column name or list of column names.
            raw (bool, optional): Set to True for column name to be included in the SQL verbatim, default is False.
            value_params (iterable, optional): List of value params if ``raw`` is True. Default is None.

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
        assert value_params is None or isinstance(value_params, collections.Iterable)

        if not isinstance(list_or_name, basestring):
            for c in list_or_name:
                self.column(c, raw, value_params)
        elif raw is True:
            self._select_expr.append((list_or_name, value_params))
        elif list_or_name not in self._select_col:
            self._select_col.append(list_or_name)

        return self

    select = column
    """Alias for :py:meth:`column`"""

    columns = column
    """Alias for :py:meth:`column`"""

    def column_expr(self, list_or_expr, value_params=None):
        """Add expressions to select.

        Arguments:
            list_or_expr (string or list): Expression or list of expressions.
            value_params (iterable, optional): List of value params. Default is None.

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
        return self.column(list_or_expr, True, value_params)

    select_expr = column_expr
    """Alias for :py:meth:`column_expr`"""

    columns_expr = column_expr
    """Alias for :py:meth:`column_expr`"""

    def remove_column(self, list_or_name):
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
        if not isinstance(list_or_name, basestring):
            for c in list_or_name:
                self.remove_column(c)
        else:
            expr_alias = ' AS {0}'.format(list_or_name)
            self._select_col = [c for c in self._select_col if c != list_or_name]
            self._select_expr = [c for c in self._select_expr if not c[0].endswith(expr_alias)]

        return self

    def qualify_columns(self, table_name, qualify_cols=None):
        """Qualify column names with a table name.

        Arguments:
            table_name (string): Table name
            qualify_cols (Iterable, optional): Column names to qualify,
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
            if qualify_cols is None or col in qualify_cols:
                if '.' not in col:
                    self._select_col[i] = '{0}.{1}'.format(table_name, col)

        return self

    def order_by(self, list_or_name):
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
        if not isinstance(list_or_name, basestring):
            for c in list_or_name:
                self.order_by(c)
        else:
            self._orderby_conds.append(list_or_name)

        return self

    def group_by(self, list_or_name):
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
        if not isinstance(list_or_name, basestring):
            for c in list_or_name:
                self.group_by(c)
        else:
            self._groupby_conds.append(list_or_name)

        return self

    def limit(self, row_count, offset=0):
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

    def having_value(self, field_or_dict, value_or_tuple=None, operator='='):
        """Compare field to a value.

        Field names may be escaped with backticks.
        Use :py:meth:`having_expr` if you want field names to be
        included in the SQL statement verbatim.

        Values will be pickled by :py:meth:`mysqlstmt.stmt.Stmt.pickle`.
        Use :py:meth:`having_raw_value` if you want values to be
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
        self.get_having_cond().where_value(field_or_dict, value_or_tuple, operator)
        return self

    having_values = having_value
    """Alias for :py:meth:`having_value`"""

    def having_raw_value(self, field_or_dict, value_or_tuple=None, operator='=', value_params=None):
        """Compare field to a an unmanipulated value.

        Field names may be escaped with backticks.
        Use :py:meth:`having_expr` if you want field names to be
        included in the SQL statement verbatim.

        Values will be included in the SQL statement verbatim.
        Use :py:meth:`having_value` if you want values to be pickled.

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
            >>> q.from_table('t1').having_raw_value('t1c1', 'NOW()').sql()
            ('SELECT * FROM t1 HAVING `t1c1` = NOW()', None)

            >>> q = Select()
            >>> q.from_table('t1').having_raw_value('t1c1', 'PASSWORD(?)', value_params=('mypw',)).sql()
            ('SELECT * FROM t1 WHERE `t1c1` = PASSWORD(?)', ['mypw'])
        """
        self.get_having_cond().where_raw_value(field_or_dict, value_or_tuple, operator, value_params)
        return self

    having_raw_values = having_raw_value
    """Alias for :py:meth:`having_raw_value`"""

    def having_expr(self, list_or_expr, expr_params=None):
        """Include a complex expression in conditional statement.

        Expressions will be included in the SQL statement verbatim.
        Use :py:meth:`having_value` or :py:meth:`having_raw_value` if you are
        doing basic field comparisons and/or want the value to be pickled.

        Arguments:
            list_or_expr (string or list): An expression or :py:class:`list` of expressions.
                Expression values can also be a tuple ``(expression, expr_params)``.
            expr_params (iterable, optional): List of expression params. Default is None.

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

    def get_having_cond(self, index=-1):
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

    def having_cond(self, cond=None, where_predicate=None):
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

    def having_and(self):
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

    def having_or(self):
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

    def sql(self):
        """Build SELECT SQL statement.

        Returns:
            Either a tuple ``(SQL statement, parameterized values)`` if ``placeholder`` is set,
            otherwise SQL statement as string.

        Raises:
            ValueError: The statement cannot be created with the given attributes.
        """

        table_refs = []
        param_values = []

        cols = [self.quote_col_ref(c) for c in self._select_col]

        for c in self._select_expr:
            expr, val_params = c
            cols.append(expr)
            if val_params is not None and self.placeholder:
                param_values.extend(val_params)

        if self._table_factors:
            table_refs.append(', '.join(self._table_factors))

        if self._join_refs:
            if not table_refs:
                raise ValueError('A root table must be specified when using joins')

            self._append_join_table_refs(self._table_factors[0], table_refs)

        # MySQL SELECT syntax as of 5.7:
        #
        # SELECT
        #     [ALL | DISTINCT | DISTINCTROW ]
        #       [HIGH_PRIORITY]
        #       [STRAIGHT_JOIN]
        #       [SQL_SMALL_RESULT] [SQL_BIG_RESULT] [SQL_BUFFER_RESULT]
        #       [SQL_CACHE | SQL_NO_CACHE] [SQL_CALC_FOUND_ROWS]
        #     select_expr [, select_expr ...]
        #     [FROM table_references
        #     [WHERE where_condition]
        #     [GROUP BY {col_name | expr | position}
        #       [ASC | DESC], ... [WITH ROLLUP]]
        #     [HAVING where_condition]
        #     [ORDER BY {col_name | expr | position}
        #       [ASC | DESC], ...]
        #     [LIMIT {[offset,] row_count | row_count OFFSET offset}]
        #     [PROCEDURE procedure_name(argument_list)]
        #     [INTO OUTFILE 'file_name' export_options
        #       | INTO DUMPFILE 'file_name'
        #       | INTO var_name [, var_name]]
        #     [FOR UPDATE | LOCK IN SHARE MODE]]

        sql = ['SELECT']

        if self.query_options:
            sql.extend(self.query_options)

        if self.cacheable is True:
            sql.append('SQL_CACHE')
        elif self.cacheable is False:
            sql.append('SQL_NO_CACHE')

        if self.calc_found_rows is True:
            sql.append('SQL_CALC_FOUND_ROWS')

        sql.append(', '.join(cols) if cols else '*')

        if table_refs:
            sql.append('FROM')
            sql.append(' '.join(table_refs))

        if self._where_cond_root.has_conds:
            sql.append('WHERE')
            sql.append(self._where_cond_root.sql(param_values))

        if self._groupby_conds:
            sql.append('GROUP BY')
            sql.append(', '.join(self._groupby_conds))

        if self._having_cond_root.has_conds:
            sql.append('HAVING')
            sql.append(self._having_cond_root.sql(param_values))

        if self._orderby_conds:
            sql.append('ORDER BY')
            sql.append(', '.join(self._orderby_conds))

        if self._limit is not None:
            row_count, offset = self._limit
            if offset > 0:
                sql.append('LIMIT {0},{1}'.format(offset, row_count))
            else:
                sql.append('LIMIT {0}'.format(row_count))

        if self.placeholder:
            return ' '.join(sql), param_values if param_values else None
        assert not param_values
        return ' '.join(sql)
