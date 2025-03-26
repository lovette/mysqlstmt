"""mysqlstmt stmt class module.

This module provides:
- Stmt
"""

from __future__ import annotations

import datetime
from collections.abc import Collection, Sequence
from typing import TYPE_CHECKING
from typing import Union as UnionT

from .config import Config

if TYPE_CHECKING:
    from typing_extensions import Self


SQLReturnParamT = UnionT[Sequence[str], None]
SQLReturnT = UnionT[str, tuple[str, SQLReturnParamT]]
SQLPReturnT = tuple[str, SQLReturnParamT]
StmtParamValueT = UnionT[str, float, bool, datetime.datetime, datetime.date, datetime.time]  # ,object
StmtPickleT = UnionT[StmtParamValueT, None]
StmtParamValuesT = Sequence[StmtParamValueT]
ValueParamsT = Sequence[str]
SelectExprT = UnionT[str, Sequence[str]]


class Stmt:
    """Base class for all statement classes."""

    def __init__(
        self,
        placeholder: str | bool | None = None,
        quote_all_col_refs: bool | None = None,
        quote_all_values: bool | None = None,
        **kwargs,
    ) -> None:
        """Constructor.

        Keyword Arguments:
            placeholder (string|bool, optional): Placeholder character to use when parameterization is enabled.
                Default is None, in which case the :py:class:`mysqlstmt.config.Config` setting will be used;
                False to disable parameterization.
            quote_all_col_refs (bool, optional): Quote all column references.
                Default is None, in which case the :py:class:`mysqlstmt.config.Config` setting will be used.
            quote_all_values (bool, optional): Quote all values when not using placeholders.
                Default is None, in which case the :py:class:`mysqlstmt.config.Config` setting will be used.
            **kwargs: Base class arguments.

        Note:
            Default settings for ``placeholder``, ``quote_all_col_refs`` and ``quote_all_values``
            are set through :py:class:`mysqlstmt.config.Config`
        """
        super().__init__(**kwargs)

        if placeholder is None:
            self.placeholder = Config.placeholder
        elif isinstance(placeholder, bool):
            self.placeholder = Config.placeholder if placeholder else None
        else:
            self.placeholder = placeholder or Config.placeholder

        self.quote_all_values = Config.quote_all_values if quote_all_values is None else quote_all_values

        self.quote_all_col_refs = Config.quote_all_col_refs if quote_all_col_refs is None else quote_all_col_refs

        # Public properties
        self.query_options = []  # can append with ``set_option``

    def __call__(self, *args, **kwargs) -> SQLPReturnT:  # noqa: ARG002
        """Returns SQL statement created by :py:meth:`sqlp`."""
        return self.sqlp()

    def __str__(self) -> str:
        """Returns SQL statement (without params) created by :py:meth:`sqlp`."""
        sql_t, _ = self.sqlp()
        return sql_t

    def sql(self) -> SQLReturnT:
        """Returns SQL statement and optionally, parameterized values.

        This method is deprecated and `sqlp` should be used instead.

        Returns:
            Either a tuple ``(SQL statement, parameterized values)`` if ``placeholder`` is set,
            otherwise SQL statement as string.

        Raises:
            ValueError: The statement cannot be created with the given attributes.
        """
        sql_, param_values = self.sqlp()
        if self.placeholder:
            return sql_, param_values
        assert not param_values
        return sql_

    def sqlp(self) -> SQLPReturnT:
        """Returns SQL statement and parameterized values.

        Derived classes must override and build appropriate SQL statement.

        Returns:
            tuple ``(SQL statement, [parameterized values])``

        Raises:
            ValueError: The statement cannot be created with the given attributes.
        """
        raise NotImplementedError

    def quote_col_ref(self, col_ref: str) -> str:
        """Quote column reference with backticks.

        Arguments:
            col_ref (string): Column reference. Can be prefixed with the table name.

        Returns:
            string: Column reference quoted with backticks (``).

        Notes:
            Column reference will not be quoted if it contains a backtick, space or parenthesis.
        """
        if self.quote_all_col_refs:
            # '*' = TABLE.*
            # ' ' = COLUMN AS ALIAS
            # '(' = FUNCTION(COLUMN)
            # '`' = Already quoted
            if any(c in col_ref for c in ("*", " ", "(", "`")):
                return col_ref

            col_ref_parts = col_ref.split(".", 1)
            if len(col_ref_parts) == 2:  # noqa: PLR2004
                table, col = col_ref_parts
                return f"{table}.`{col}`"

            return f"`{col_ref}`"

        return col_ref

    def pickle(self, val: StmtPickleT) -> tuple[str, bool]:  # noqa: PLR0911
        """Convert variable value into a value that can be included in a SQL statement.

        Arguments:
            val (mixed): Value to pickle.

        Returns:
            tuple: (string, bool) Pickled value as a string and True if value should be parameterized.
        """
        if val is None:
            return "NULL", False
        if val is True:
            return "1", False
        if val is False:
            return "0", False
        if isinstance(val, str):
            return val, True
        if isinstance(val, (int, float)):
            return str(val), False
        if isinstance(val, datetime.datetime):
            return val.strftime("%Y-%m-%d %H:%M:%S"), True
        if isinstance(val, datetime.date):
            return val.strftime("%Y-%m-%d"), True
        if isinstance(val, datetime.time):
            return val.strftime("%H:%M:%S"), True
        return str(val), True

    @staticmethod
    def quote(val: str) -> str:
        """Quotes a string with single quotemarks and adds backslashes to escape embedded single quotes.

        Arguments:
            val (string): Column reference. Can be prefixed with the table name.

        Returns:
            string: Column reference quoted with backticks (``).

        Note:
            This is a very simple implementation. Conventional wisdom says you should *never* need
            to use this functionality. Whenever possible you should use parameterization,
            or escape values before they get to creating SQL statments.
        """
        return "'{}'".format(val.replace("'", "\\'"))

    @staticmethod
    def table_alias(table_factor: str) -> str:
        """Returns the table alias from a table factor.

        Arguments:
            table_factor (string): Table factor reference such as ``table`` or ``table AS alias``.

        Returns:
            string
        """
        table_parts = table_factor.split("AS")
        return table_factor if len(table_parts) == 1 else table_parts[1].strip()

    def parameterize_values(
        self,
        list_or_value: StmtPickleT | Sequence[StmtPickleT],
        inline_values: list[str] | None,
        param_values: list[str] | None,
    ) -> None:
        """Parameterizes a value or list of values.

        Evaluates or iterates through ``list_or_value`` and if the value can be parameterized
        it is added to ``param_values``, otherwise it is added to ``inline_values``.

        Arguments:
            list_or_value (mixed or collection): A value or collection of values to replace with ``placeholder``.
            inline_values (list or None, modified): List to append non-parameterized values to;
                set to None to force everything to be parameterized.
            param_values (list or None, modified): List to append parameterized values to;
                set to None to force everything not to be inlined.
        """
        if isinstance(list_or_value, Sequence) and not isinstance(list_or_value, str):
            for val in list_or_value:
                self.parameterize_values(val, inline_values, param_values)
        else:
            using_placeholder = (param_values is not None) and bool(self.placeholder)
            quote = False if using_placeholder is True else self.quote_all_values

            list_or_value, can_paramize_val = self.pickle(list_or_value)

            if inline_values is not None:
                if can_paramize_val and param_values is not None and self.placeholder:
                    inline_values.append(self.placeholder)
                    param_values.append(list_or_value)
                elif can_paramize_val and quote:
                    inline_values.append(self.quote(list_or_value))
                else:
                    inline_values.append(list_or_value)
            elif param_values is not None:
                param_values.append(list_or_value)
            else:
                errmsg = "Either 'inline_values' or 'param_values' arguments must not be None"
                raise ValueError(errmsg)

    def set_option(self, list_or_value: str | Collection[str]) -> Self:
        """Sets query options (the keywords at the beginning of the SQL statement).

        Arguments:
            list_or_value (mixed): An option or collection of options.

        Returns:
            object: self
        """
        if isinstance(list_or_value, Collection) and not isinstance(list_or_value, str):
            for val in list_or_value:
                self.set_option(val)
        else:
            self.query_options.append(list_or_value)

        return self
