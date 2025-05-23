"""mysqlstmt insert class module.

This module provides:
- Insert
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from .select import Select
from .set_values_mixin import SetValuesMixin
from .stmt import Stmt, StmtPickleT

if TYPE_CHECKING:
    from .stmt import SQLPReturnT


SetBatchValueT = Sequence[StmtPickleT]


class Insert(Stmt, SetValuesMixin):
    """INSERT statement.

    Attributes:
        ignore_error (bool): Include the IGNORE option in the INSERT statement.

    Examples: ::

        >>> q = Insert('t1')
        >>> q.set_value('t1c1', 1).sql()
        ('INSERT INTO t1 (`t1c1`) VALUES (1)', None)

        >>> q = Insert('t1', ignore_error=True)
        >>> q.set_value('t1c1', 1).sql()
        ('INSERT IGNORE INTO t1 (`t1c1`) VALUES (1)', None)

        >>> q = Insert('t1')
        >>> q.set_option('LOW_PRIORITY').set_value('t1c1', 1).sql()
        ('INSERT LOW_PRIORITY INTO t1 (`t1c1`) VALUES (1)', None)

    See Also:
        :py:class:`mysqlstmt.replace.Replace`
    """

    def __init__(
        self,
        table_name: str | None = None,
        ignore_error: bool = False,
        select_allow_placeholders: bool = False,
        **kwargs,
    ) -> None:
        """Constructor.

        Keyword Arguments:
            table_name (string, optional): Table to insert into.
            ignore_error (bool, optional): Include IGNORE flag in statement.
            select_allow_placeholders (bool, optional): Allow SELECT in INSERT...SELECT to use placeholders? Default is False.
            **kwargs: Base class arguments.
        """
        super().__init__(**kwargs)

        # Public flags
        self.ignore_error = ignore_error
        self.select_allow_placeholders = select_allow_placeholders

        # Internals
        self._table_name = None
        self._columns: list[str] = []
        self._select: str | Select | None = None
        self._batch_values: list[SetBatchValueT] = []
        self._replace = False

        if table_name:
            self.into_table(table_name)

    def into_table(self, table_name: str) -> Insert:
        """Set table to insert into.

        Arguments:
            table_name (string): Name of table.

        Returns:
            object: self

        Raises:
            ValueError: More than one table was specified.
        """
        if not isinstance(table_name, str):
            msg = "Only one table can be specified"
            raise ValueError(msg)  # noqa: TRY004

        self._table_name = table_name
        return self

    def column(self, list_or_name: str | Sequence[str]) -> Insert:
        """Add column names to insert into.

        Arguments:
            list_or_name (string or list): Column name or list of column names.

        Returns:
            object: self
        """
        if not isinstance(list_or_name, str):
            for c in list_or_name:
                self.column(c)
        else:
            self._columns.append(list_or_name)
        return self

    columns = column
    """Alias for :py:meth:`column`"""

    def select(self, stmt: str | Select) -> Insert:
        """Insert rows resulting from a SELECT statement.

        Arguments:
            stmt (string or mysqlstmt.Select): SQL or a :py:class:`mysqlstmt.select.Select` statement to execute.

        Returns:
            object: self

        Examples: ::

            >>> q = Insert()
            >>> q.into_table('t1').columns('t1c1').select('SELECT t2c1 FROM t2').sql()
            ('INSERT INTO t1 (`t1c1`) SELECT t2c1 FROM t2', None)

            >>> q = Insert()
            >>> q.into_table('t1').columns(['t1c1','t1c2']).select('SELECT `t2c1`, `t2c2` FROM t2').sql()
            ('INSERT INTO t1 (`t1c1`, `t1c2`) SELECT `t2c1`, `t2c2` FROM t2', None)

            >>> q = Insert()
            >>> qselect = Select('t2').columns(['t2c1', 't2c2'])
            >>> q.into_table('t1').columns(['t1c1','t1c2']).select(qselect).sql()
            ('INSERT INTO t1 (`t1c1`, `t1c2`) SELECT `t2c1`, `t2c2` FROM t2', None)
        """
        self._select = stmt
        return self

    def set_batch_value(self, values: Sequence[SetBatchValueT]) -> Insert:
        """Set batch values.

        Sets values for multiple rows at once.

        Arguments:
            values (list): List of row values; each row value must be a collection of column values.

        Returns:
            object: self

        Examples: ::

            >>> q = Insert()
            >>> data = [['v1']]
            >>> q.into_table('t1').columns('t1c1').set_batch_value(data).sql()
            ("INSERT INTO t1 (`t1c1`) VALUES (?)", data)

            >>> q = Insert()
            >>> data = [['v1'],['v2'],['NOW()']]
            >>> q.into_table('t1').columns('t1c1').set_batch_value(data).sql()
            ("INSERT INTO t1 (`t1c1`) VALUES (?)", data)

            >>> q = Insert()
            >>> data = [['v1','v2', 'NOW()'], ['v1','v2', 'NOW()'], ['v1','v2', 'NOW()']]
            >>> q.into_table('t1').columns(['t1c1','t1c2','t1c3']).set_batch_value(data).sql()
            ("INSERT INTO t1 (`t1c1`, `t1c2`, `t1c3`) VALUES (?, ?, ?)", data)

            >>> q = Insert(placeholder=False)
            >>> data = [["'v1'"]]
            >>> q.into_table('t1').columns('t1c1').set_batch_value(data).sql()
            "INSERT INTO t1 (`t1c1`) VALUES ('v1')"

            >>> q = Insert(placeholder=False)
            >>> data = [["'v1'"],["'v2'"],['NOW()']]
            >>> q.into_table('t1').columns('t1c1').set_batch_value(data).sql()
            "INSERT INTO t1 (`t1c1`) VALUES ('v1'), ('v2'), (NOW())"

            >>> q = Insert(placeholder=False)
            >>> data = [["'r1v1'","'r1v2'", 'NOW()'], ["'r2v1'","'r2v2'", 'NOW()'], ["'r3v1'","'r3v2'", 'NOW()']]
            >>> q.into_table('t1').columns(['t1c1','t1c2','t1c3']).set_batch_value(data).sql()
            "INSERT INTO t1 (`t1c1`, `t1c2`, `t1c3`) VALUES ('r1v1', 'r1v2', NOW()), ('r2v1', 'r2v2', NOW()), ('r3v1', 'r3v2', NOW())"
        """
        self._batch_values.extend(values)
        return self

    set_batch_values = set_batch_value
    """Alias for :py:meth:`set_batch_value`"""

    def sqlp(self) -> SQLPReturnT:  # noqa: C901, PLR0912, PLR0915
        """Build INSERT SQL statement.

        Returns:
            Either a tuple ``(SQL statement, parameterized values)`` if ``placeholder`` is set,
            otherwise SQL statement as string.

        Raises:
            ValueError: The statement cannot be created with the given attributes.
        """
        col_names = self._columns  # Be careful not to overwrite!
        param_values = []

        if not self._table_name:
            msg = "No table is specified"
            raise ValueError(msg)

        # MySQL INSERT syntax as of 5.7:
        #
        # > INSERT [LOW_PRIORITY | HIGH_PRIORITY] [IGNORE]
        # >     [INTO] tbl_name
        # >     [PARTITION (partition_name,...)]
        # >     [(col_name,...)]
        # >     {VALUES | VALUE} ({expr | DEFAULT},...),(...),...
        # >     [ ON DUPLICATE KEY UPDATE col_name=expr [, col_name=expr] ... ]
        #
        # > INSERT [LOW_PRIORITY | HIGH_PRIORITY] [IGNORE]
        # >     [INTO] tbl_name
        # >     [PARTITION (partition_name,...)]
        # >     [(col_name,...)]
        # >     SELECT ...
        # >     [ ON DUPLICATE KEY UPDATE col_name=expr [, col_name=expr] ... ]

        sql = ["REPLACE" if self._replace else "INSERT"]

        if self.query_options:
            sql.extend(self.query_options)

        if self.ignore_error:
            sql.append("IGNORE")

        sql.append("INTO")
        sql.append(self._table_name)

        if self._values or self._values_raw:
            if col_names:
                msg = "columns cannot be explicitly set when set_value or set_raw_value is used"
                raise ValueError(msg)
            if self._batch_values:
                msg = "set_batch_value is incompatible with set_value and set_raw_value"
                raise ValueError(msg)
            if self._select:
                msg = "set_value and set_raw_value are incompatible with INSERT...SELECT"
                raise ValueError(msg)

            col_names = []
            inline_values = []

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

            sql.append(f"({', '.join([self.quote_col_ref(col) for col in col_names])})")
            sql.append(f"VALUES ({', '.join(inline_values)})")

        elif self._batch_values:
            if not col_names:
                msg = "columns must be explicitly set when set_batch_value is used"
                raise ValueError(msg)
            if self._select:
                msg = "set_batch_value is incompatible with INSERT...SELECT"
                raise ValueError(msg)

            sql.append(f"({', '.join([self.quote_col_ref(col) for col in col_names])})")

            if not self.placeholder:
                inline_values = []

                for row in self._batch_values:
                    row_values = []
                    self.parameterize_values(row, row_values, None)
                    inline_values.append(f"({', '.join(row_values)})")

                sql.append(f"VALUES {', '.join(inline_values)}")
            else:
                # ALL columns are parameterized
                placeholder = str(self.placeholder)
                inline_values = [placeholder for _ in range(len(col_names))]
                assert len(col_names) == len(inline_values)
                sql.append(f"VALUES ({', '.join(inline_values)})")

                for row in self._batch_values:
                    row_param_value = []
                    self.parameterize_values(row, None, row_param_value)
                    param_values.append(row_param_value)

        elif self._select:
            if not col_names:
                msg = "No columns are specified"
                raise ValueError(msg)

            sql.append(f"({', '.join([self.quote_col_ref(col) for col in col_names])})")

            if isinstance(self._select, Select):
                select_sql, select_params = self._select.sqlp()

                if select_params is not None:
                    if not self.select_allow_placeholders:
                        msg = "INSERT...SELECT cannot use parameterized SELECT"
                        raise ValueError(msg)

                    param_values.extend(select_params)

                sql.append(select_sql)
            else:
                sql.append(self._select)

        else:
            msg = "No values are specified"
            raise ValueError(msg)

        if self.placeholder:
            return " ".join(sql), tuple(param_values) if param_values else None
        assert not param_values
        return " ".join(sql), None
