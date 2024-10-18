import mysqlstmt


class Union(mysqlstmt.Stmt):
    """SELECT... UNION statement.

    Examples: ::

        >>> q = Union()
        >>> sql_t = q.union('SELECT `t1c1` FROM t1').select('SELECT `t2c1` FROM t2').sql()
        ('(SELECT `t1c1` FROM t1) UNION (SELECT `t2c1` FROM t2)', None)

        >>> s1 = Select('t1').column('t1c1')
        >>> s2 = Select('t2').column('t2c1')
        >>> q = Union()
        >>> sql_t = q.union([s1, s2]).sql()
        ('(SELECT `t1c1` FROM t1) UNION (SELECT `t2c1` FROM t2)', None)

        >>> s1 = Select('t1').column('t1c1')
        >>> s2 = Select('t2').column('t2c1')
        >>> q = Union([s1, s2])
        >>> sql_t = q.sql()
        ('(SELECT `t1c1` FROM t1) UNION (SELECT `t2c1` FROM t2)', None)
    """

    def __init__(self, list_or_stmt=None, distinct=None, **kwargs):
        """Constructor

        Keyword Arguments:
            list_or_stmt(string or list, optional): A single, or list of, 'SELECT...' SQL or :py:class:`mysqlstmt.select.Select` statements to execute.
            distinct (bool, optional): Include DISTINCT or ALL option in statement.
            **kwargs: Base class arguments.
        """
        super(Union, self).__init__(**kwargs)

        self._selects = []
        self._distinct = distinct
        self._orderby_conds = []
        self._limit = None

        if list_or_stmt is not None:
            self.union(list_or_stmt)

    def union(self, list_or_stmt):
        """Add SELECT statement to union.

        Arguments:
            list_or_stmt (string or mysqlstmt.Select): A single, or list of, 'SELECT...' SQL or :py:class:`mysqlstmt.select.Select` statements to execute.

        Returns:
            object: self

        Examples: ::

            >>> s = Select('t1').column('t1c1')
            >>> q = Union()
            >>> sql_t = q.union(s).sql()
            ('(SELECT `t1c1` FROM t1)', None)

            >>> s1 = Select('t1').column('t1c1')
            >>> s2 = Select('t2').column('t2c1')
            >>> q = Union()
            >>> sql_t = q.union(s1).union(s2).sql()
            ('(SELECT `t1c1` FROM t1) UNION (SELECT `t2c1` FROM t2)', None)
        """
        if not isinstance(list_or_stmt, basestring) and not isinstance(list_or_stmt, mysqlstmt.Select):
            for c in list_or_stmt:
                self.union(c)
        else:
            self._selects.append(list_or_stmt)
        return self

    select = union
    """Alias for :py:meth:`union`."""

    def order_by(self, list_or_name):
        """Add expressions to order by.

        Arguments:
            list_or_name (string or list): An expression or list of expressions to order by.

        Returns:
            object: self

        Examples: ::

            >>> q = Union()
            >>> sql_t = q.union('SELECT `t1c1` AS sort_col FROM t1').select('SELECT `t2c1` FROM t2').order_by('sort_col, DESC').sql()
            ('(SELECT `t1c1` AS sort_col FROM t1) UNION (SELECT `t2c1` FROM t2) ORDER BY sort_col, DESC', None)
        """
        if not isinstance(list_or_name, basestring):
            for c in list_or_name:
                self.order_by(c)
        else:
            self._orderby_conds.append(list_or_name)

        return self

    def limit(self, row_count, offset=0):
        """Add limit clause expression.

        Arguments:
            row_count (int): Maximum number of rows to return.
            offset (int, optional): Offset of the first row to return.

        Returns:
            object: self

        Examples: ::

            >>> q = Union()
            >>> sql_t = q.union('SELECT `t1c1` AS sort_col FROM t1').select('SELECT `t2c1` FROM t2').order_by('sort_col, DESC').limit(5).sql()
            ('(SELECT `t1c1` AS sort_col FROM t1) UNION (SELECT `t2c1` FROM t2) ORDER BY sort_col, DESC LIMIT 5', None)
        """
        self._limit = (row_count, offset)
        return self

    def sql(self):
        """Build SELECT... UNION SQL statement.

        Returns:
            Either a tuple ``(SQL statement, parameterized values)`` if ``placeholder`` is set,
            otherwise SQL statement as string.

        Raises:
            ValueError: The statement cannot be created with the given attributes.
        """
        if not self._selects:
            raise ValueError('No SELECT statements are specified')

        sql = []
        param_values = []

        # MySQL SELECT syntax as of 5.7:
        #
        # SELECT ...
        # UNION [ALL | DISTINCT] SELECT ...
        # [UNION [ALL | DISTINCT] SELECT ...]

        if self.query_options:
            sql.extend(self.query_options)

        for stmt in self._selects:
            if isinstance(stmt, mysqlstmt.Select):
                select_sql, select_params = stmt.sql()
                stmtsql = select_sql
                if select_params is not None:
                    param_values.extend(select_params)
            else:
                stmtsql = stmt

            if sql:
                if self._distinct is False:
                    sql.append('UNION ALL')
                else:
                    sql.append('UNION')

            sql.append(u'({0})'.format(stmtsql))

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
