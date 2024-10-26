"""mysqlstmt where_condition class module.

This module provides:
- WhereCondition
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING

from .stmt import Stmt

if TYPE_CHECKING:
    import datetime
    from collections.abc import Mapping

    from .where_condition import WhereCondition
    from .where_mixin import WhereMixin


class WhereCondition:
    """Condition statements that can be used for WHERE and HAVING clauses.

    ``WhereCondition`` objects are created automatically by the ``where`` and ``having`` functions
    of statement classes, but you can create your own complex conditions, even nested conditions, using
    ``WhereCondition`` objects.
    """

    def __init__(self, stmt: WhereMixin, where_predicate: str | None = None, **kwargs) -> None:
        """Constructor.

        Keyword Arguments:
            stmt (WhereMixin): Statement this condition is associated with.
            where_predicate (string, optional): The predicate for this condition, either 'AND' or 'OR'.
            **kwargs: Base class arguments.
        """
        super().__init__(**kwargs)

        assert isinstance(stmt, Stmt)
        self._stmt = stmt

        # > AND {field: (value, operator), ...}
        # >  OR [(field, (value, operator)), ...]
        self._values: (
            dict[str, tuple[str | float | datetime.datetime | datetime.date | datetime.time | None, str]]
            | list[tuple[str, tuple[str | float | datetime.datetime | datetime.date | datetime.time | None, str]]]
        )

        # > AND {field: (value, operator, params), ...}
        # >  OR [(field, (value, operator, params)), ...]
        # with 'params' being sequence of values or None.
        self._values_raw: (
            dict[
                str,
                tuple[
                    str | float | datetime.datetime | datetime.date | datetime.time | None,
                    str,
                    Sequence[str | float | datetime.datetime | datetime.date | datetime.time] | None,
                ],
            ]
            | list[
                tuple[
                    str,
                    tuple[
                        str | float | datetime.datetime | datetime.date | datetime.time | None,
                        str,
                        Sequence[str | float | datetime.datetime | datetime.date | datetime.time] | None,
                    ],
                ]
            ]
        )

        if where_predicate is None or where_predicate == "AND":
            # With 'AND', it makes sense to only set one value per field
            # so we use a dict: field=(value, operator, value_params)
            self._values = {}
            self._values_raw = {}
            where_predicate = "AND"
        elif where_predicate == "OR":
            # With 'OR', you can reference the same field multiple times
            # so we use a list of tuples: (field, (value, operator, value_params))
            self._values = []
            self._values_raw = []
        else:
            msg = "where_predicate must be 'AND' or 'OR'"
            raise ValueError(msg)

        self._conds = []
        self._raw_exprs = []
        self._predicate = f" {where_predicate} "
        self._nesting_level = 0

    @property
    def expr_count(self) -> int:
        """Count the number of expressions in this condition.

        Returns:
            int: Number of values and conditions that will result in an expression.
        """
        c = len(self._values) + len(self._values_raw) + len(self._raw_exprs)
        for cond in self._conds:
            if cond.has_conds:
                c += 1
        return c

    @property
    def has_conds(self) -> bool:
        """Check if this condition will result in an expression.

        Returns:
            bool: True if this condition has value or conditions that will result in an expression, otherwise False.
        """
        if self._values or self._values_raw or self._raw_exprs:
            return True

        return any(cond.has_conds for cond in self._conds)

    @property
    def nesting_level(self) -> int:
        """The nesting_level of this condition.

        Note:
            This is set automatically when the condition is added to a statement or another condition.
        """
        return self._nesting_level

    @nesting_level.setter
    def nesting_level(self, value: int) -> None:
        self._nesting_level = value

        # Set nesting level for subconditions
        for cond in self._conds:
            cond.nesting_level = self._nesting_level + 1

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
            :py:class:`mysqlstmt.where_condition.WhereCondition` :py:meth:`add_cond`
        """
        return self._conds[index]

    def add_cond(self, cond: WhereCondition | None = None, where_predicate: str | None = None) -> WhereCondition:
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
        if cond is None:
            cond = WhereCondition(self._stmt, where_predicate=where_predicate)
        assert isinstance(cond, WhereCondition)
        cond.nesting_level = self.nesting_level + 1
        self._conds.append(cond)
        return self

    def where_and(self) -> WhereCondition:
        """Activates a new ``WhereCondition`` with an 'AND' predicate.

        Returns:
            object: self

        See Also:
            :py:class:`mysqlstmt.where_condition.WhereCondition` :py:meth:`add_cond` :py:meth:`where_or`
        """
        return self.add_cond(where_predicate="AND")

    def where_or(self) -> WhereCondition:
        """Activates a new ``WhereCondition`` with an 'OR' predicate.

        Returns:
            object: self

        See Also:
            :py:class:`mysqlstmt.where_condition.WhereCondition` :py:meth:`where_cond` :py:meth:`where_and`
        """
        return self.add_cond(where_predicate="OR")

    def where_value(
        self,
        field_or_dict: str | Mapping[str, str | float | datetime.datetime | datetime.date | datetime.time | None],
        value_or_tuple: str
        | float
        | tuple[str | float | datetime.datetime | datetime.date | datetime.time | None, str]
        | list[str | float | datetime.datetime | datetime.date | datetime.time]
        | object
        | None = None,
        operator: str = "=",
    ) -> WhereCondition:
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
        """
        assert isinstance(field_or_dict, (str, dict))
        assert isinstance(operator, str)

        if not isinstance(field_or_dict, str):
            for f, v in field_or_dict.items():
                self.where_value(f, v, operator)
        elif not isinstance(value_or_tuple, tuple):
            self.where_value(field_or_dict, (value_or_tuple, operator))
        elif isinstance(self._values, dict):
            self._values[field_or_dict] = value_or_tuple
        else:
            self._values.append((field_or_dict, value_or_tuple))

        return self

    def where_raw_value(
        self,
        field_or_dict: str | Mapping[str, str | float | datetime.datetime | datetime.date | datetime.time | None],
        value_or_tuple: str
        | float
        | datetime.datetime
        | datetime.date
        | datetime.time
        | None
        | tuple[
            str | float | datetime.datetime | datetime.date | datetime.time | None,
            str,
            Sequence[str | float | datetime.datetime | datetime.date | datetime.time] | None,
        ] = None,
        operator: str = "=",
        value_params: Sequence[str | float | datetime.datetime | datetime.date | datetime.time] | None = None,
    ) -> WhereCondition:
        """Compare field to a an unmanipulated value.

        Field names may be escaped with backticks.
        Use :py:meth:`where_expr` if you want field names to be
        included in the SQL statement verbatim.

        Values will be included in the SQL statement verbatim.
        Use :py:meth:`where_value` if you want values to be pickled.

        Parameterized values will be pickled by :py:meth:`mysqlstmt.stmt.Stmt.pickle`.

        Arguments:
            field_or_dict (string or list): Name of field/column or :py:class:`dict` mapping fields to values.
                Dictionary values can also be a tuple, as described below.
            value_or_tuple (string or tuple, optional): Value to compare with if ``field_or_dict`` is a field name.
                Can also be a tuple ``(value, operator, value_params)``.
            operator (string, optional): Comparison operator, default is '='.
            value_params (iterable, optional): List of value params. Default is None.

        Returns:
            object: self
        """
        assert isinstance(field_or_dict, (str, dict))
        assert value_or_tuple is None or isinstance(value_or_tuple, (str, tuple))
        assert isinstance(operator, str)
        assert value_params is None or isinstance(value_params, Iterable)

        if not isinstance(field_or_dict, str):
            for f, v in field_or_dict.items():
                self.where_raw_value(f, v)
        elif not isinstance(value_or_tuple, tuple):
            self.where_raw_value(field_or_dict, (value_or_tuple, operator, value_params))
        elif isinstance(self._values_raw, dict):
            assert isinstance(value_or_tuple, tuple)
            assert len(value_or_tuple) == 3  # noqa: PLR2004
            self._values_raw[field_or_dict] = value_or_tuple
        else:
            assert isinstance(value_or_tuple, tuple)
            assert len(value_or_tuple) == 3  # noqa: PLR2004
            self._values_raw.append((field_or_dict, value_or_tuple))

        return self

    def where_expr(
        self,
        expr_or_list: str | list[str] | tuple[str, Sequence[str] | None],
        expr_params: Sequence[str] | None = None,
    ) -> WhereCondition:
        """Include a complex expression in conditional statement.

        Expressions will be included in the SQL statement verbatim.
        Use :py:meth:`where_value` or :py:meth:`where_raw_value` if you are
        doing basic field comparisons and/or want the value to be pickled.

        Arguments:
            expr_or_list (string or list): An expression or :py:class:`list` of expressions.
                Expression values can also be a tuple ``(expression, expr_params)``.
            expr_params (iterable, optional): List of expression params. Default is None.

        Returns:
            object: self
        """
        assert isinstance(expr_or_list, (str, list, tuple))
        assert expr_params is None or isinstance(expr_params, Iterable)

        if not isinstance(expr_or_list, str) and not isinstance(expr_or_list, tuple):
            for expr in expr_or_list:
                self.where_expr(expr)
        elif not isinstance(expr_or_list, tuple):
            self.where_expr((expr_or_list, expr_params))
        else:
            assert isinstance(expr_or_list, tuple)
            assert len(expr_or_list) == 2  # noqa: PLR2004
            self._raw_exprs.append(expr_or_list)

        return self

    def sql(self, param_values: list[str]) -> str | None:  # noqa: C901, PLR0912, PLR0915
        """Build SQL snippet to include in a WHERE or HAVING clause.

        Returns:
            Either SQL statement as string, or ``None`` if condition is empty.

        Note:
            This function is called by the statement object that the condition is a member of.
            You won't need to use this function unless you're just curious about the SQL
            it creates.
        """
        sql = []

        for cond in self._conds:
            cond_sql = cond.sql(param_values)
            if cond_sql:
                sql.append(cond_sql)

        for field_or_tuple in self._values:
            if isinstance(self._values, dict):
                val, op = self._values[field_or_tuple]
                field = field_or_tuple
            else:
                field, value_op_tuple = field_or_tuple
                val, op = value_op_tuple

            field = self._stmt.quote_col_ref(field)
            inline_values = []

            self._stmt.parameterize_values(val, inline_values, param_values)

            if isinstance(val, Sequence) and not isinstance(val, str):
                # Force lists and tuples to be an IN statement
                if len(val) > 1:
                    val = f"({', '.join(inline_values)})"
                    if op == "=":
                        op = "IN"
                    elif op == "<>":
                        op = "NOT IN"
                else:
                    # Simplify 'FIELD IN (VALUE)' to 'FIELD = VALUE'
                    val = inline_values[0]
                    if op == "IN":
                        op = "="
                    elif op == "NOT IN":
                        op = "<>"
            else:
                val = inline_values[0]

            if val in ("NULL", "NOT NULL"):
                if op == "=":
                    op = "IS"
                elif op == "<>":
                    op = "IS NOT"

            sql.append(f"{field} {op} {val}")

        for field_or_tuple in self._values_raw:
            if isinstance(self._values_raw, dict):
                val, op, val_params = self._values_raw[field_or_tuple]
                field = field_or_tuple
            else:
                field, value_op_tuple = field_or_tuple
                val, op, val_params = value_op_tuple

            if val_params is not None and self._stmt.placeholder:
                for param_val in val_params:
                    pickled_val, can_paramize_val = self._stmt.pickle(param_val)
                    param_values.append(pickled_val)

            sql.append(f"{self._stmt.quote_col_ref(field)} {op} {val}")

        for expr_tuple in self._raw_exprs:
            expr, expr_params = expr_tuple
            sql.append(expr)
            if expr_params is not None and self._stmt.placeholder:
                param_values.extend(expr_params)

        if not sql:
            return None
        if self.expr_count > 1:
            return f"({self._predicate.join(sql)})"
        return f"{self._predicate.join(sql)}"
