"""mysqlstmt where_condition class module.

This module provides:
- WhereCondition
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Literal
from typing import Union as UnionT

from .stmt import Stmt, StmtParamValuesT, StmtPickleT, ValueParamsT

if TYPE_CHECKING:
    from .where_condition import WhereCondition
    from .where_mixin import WhereMixin


WhereOpT = str
WherePredT = Literal["AND", "OR"]

WhereValueT = UnionT[StmtPickleT, Sequence[StmtPickleT]]
WhereRawValueT = str

WhereFieldConditionT = tuple[WhereValueT, WhereOpT]
WhereFieldConditionRawT = tuple[WhereRawValueT, WhereOpT, UnionT[StmtParamValuesT, None]]
WhereFieldConditionSelectT = tuple[Stmt, WhereOpT, UnionT[StmtParamValuesT, None]]

WhereExprValuesT = tuple[str, UnionT[ValueParamsT, None]]
WhereExprT = UnionT[str, ValueParamsT]


class WhereCondition:
    """Condition statements that can be used for WHERE and HAVING clauses.

    ``WhereCondition`` objects are created automatically by the ``where`` and ``having`` functions
    of statement classes, but you can create your own complex conditions, even nested conditions, using
    ``WhereCondition`` objects.
    """

    def __init__(
        self,
        stmt: WhereMixin,
        where_predicate: WherePredT | None = None,
        negate: bool = False,
        **kwargs,
    ) -> None:
        """Constructor.

        Keyword Arguments:
            stmt (WhereMixin): Statement this condition is associated with.
            where_predicate (string, optional): The predicate for this condition, either 'AND' or 'OR'. Default is 'AND'.
            negate (bool, optional): Whether this condition is negated (as in "NOT").
            **kwargs: Base class arguments.
        """
        super().__init__(**kwargs)

        assert isinstance(stmt, Stmt)
        self._stmt = stmt
        self._negate = negate

        # > AND {field: (value, operator), ...}
        # >  OR [(field, (value, operator)), ...]
        self._values: dict[str, WhereFieldConditionT] | list[tuple[str, WhereFieldConditionT]]

        # > AND {field: (value, operator, params), ...}
        # >  OR [(field, (value, operator, params)), ...]
        self._values_raw: dict[str, WhereFieldConditionRawT] | list[tuple[str, WhereFieldConditionRawT]]

        # > AND {field: (Select, operator, params), ...}
        # >  OR [(field, (Select, operator, params)), ...]
        self._selects: dict[str, WhereFieldConditionSelectT] | list[tuple[str, WhereFieldConditionSelectT]]

        if where_predicate is None or where_predicate == "AND":
            # With 'AND', it makes sense to only set one value per field
            # so we use a dict: field=(value, operator, value_params)
            self._values = {}
            self._values_raw = {}
            self._selects = {}
            where_predicate = "AND"
        elif where_predicate == "OR":
            # With 'OR', you can reference the same field multiple times
            # so we use a list of tuples: (field, (value, operator, value_params))
            self._values = []
            self._values_raw = []
            self._selects = []
        else:
            msg = "where_predicate must be 'AND' or 'OR'"
            raise ValueError(msg)

        self._conds: list[WhereCondition] = []
        self._raw_exprs: list[WhereExprValuesT] = []
        self._predicate = f" {where_predicate} "
        self._nesting_level = 0

    def __repr__(self) -> str:
        """Return a string representation of the object for debugging purposes.

        Returns:
            str
        """
        # Note: I didn't use the result of sql() here to keep
        # what's shown in the debug view as simple as possible.

        pred = self._predicate.strip()
        repr_conds = [cond.__repr__() for cond in self._conds]

        repr_conds.extend(["VALUE"] * len(self._values))
        repr_conds.extend(["RAWVALUE"] * len(self._values_raw))
        repr_conds.extend(["EXPR"] * len(self._raw_exprs))
        repr_conds.extend(["SELECT"] * len(self._selects))

        repr_conds = [cond for cond in repr_conds if cond]  # empty conditions can be ignored

        if not repr_conds:
            repr_str = ""
        elif len(repr_conds) == 1:
            repr_str = repr_conds[0]
        else:
            repr_str = self._predicate.join(repr_conds)

        if self._negate:
            repr_str = f"NOT {repr_str}"

        if self.nesting_level == 0:
            return f"<{self.__class__.__name__}[{pred}] {repr_str}>"
        if repr_str:
            return f"({repr_str})"
        return ""

    @property
    def expr_count(self) -> int:
        """Count the number of expressions in this condition.

        Returns:
            int: Number of values and conditions that will result in an expression.
        """
        c = len(self._values) + len(self._values_raw) + len(self._raw_exprs) + len(self._selects)

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
        if self._values or self._values_raw or self._raw_exprs or self._selects:
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

    @property
    def where_cond(self) -> WhereCondition:
        """Returns the active condition.

        Returns:
            object: :py:class:`WhereCondition`
        """
        return self._conds[-1]

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

    def add_cond(
        self,
        cond: WhereCondition | None = None,
        where_predicate: WherePredT | None = None,
        negate: bool = False,
    ) -> WhereCondition:
        """Activates a new ``WhereCondition``.

        Arguments:
            cond (mysqlstmt.WhereCondition, optional): A new condition; one will be created if not specified.
            where_predicate (string): The predicate for the new condition if a new one is created, either 'AND' or 'OR'.
            negate (bool, optional): Whether this condition is negated (as in "NOT").

        Returns:
            WhereCondition: The new condition.

        Note:
            Conditions are typically created with ``where_and`` and ``where_or``.
            You should use this function when creating complex conditions with ``WhereCondition`` objects.

        See Also:
            :py:class:`mysqlstmt.where_condition.WhereCondition` :py:meth:`where_and` :py:meth:`where_or`
        """
        if cond is None:
            cond = WhereCondition(self._stmt, where_predicate=where_predicate, negate=negate)
        assert isinstance(cond, WhereCondition)
        cond.nesting_level = self.nesting_level + 1
        self._conds.append(cond)
        return cond

    def where_and(self, negate: bool = False) -> WhereCondition:
        """Activates a new ``WhereCondition`` with an 'AND' predicate.

        Arguments:
            negate (bool, optional): Whether this condition is negated (as in "NOT").

        Returns:
            WhereCondition: The new condition.

        See Also:
            :py:class:`mysqlstmt.where_condition.WhereCondition` :py:meth:`add_cond` :py:meth:`where_or`
        """
        return self.add_cond(where_predicate="AND", negate=negate)

    def where_or(self, negate: bool = False) -> WhereCondition:
        """Activates a new ``WhereCondition`` with an 'OR' predicate.

        Arguments:
            negate (bool, optional): Whether this condition is negated (as in "NOT").

        Returns:
            WhereCondition: The new condition.

        See Also:
            :py:class:`mysqlstmt.where_condition.WhereCondition` :py:meth:`where_cond` :py:meth:`where_and`
        """
        return self.add_cond(where_predicate="OR", negate=negate)

    def where_value(
        self,
        field_or_dict: str | Mapping[str, WhereValueT],
        value: WhereValueT | None = None,
        operator: WhereOpT = "=",
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
            value (mixed, optional): Value to compare with if ``field_or_dict`` is a field name.
                Type can be anything that :py:meth:`mysqlstmt.stmt.Stmt.pickle` can handle (Sequence, Object,etc.).
            operator (string, optional): Comparison operator, default is '='.

        Returns:
            object: self
        """
        assert isinstance(field_or_dict, (str, dict))
        assert isinstance(operator, str)

        if isinstance(field_or_dict, Mapping):
            for f, v in field_or_dict.items():
                self.where_value(f, v, operator)
        elif isinstance(self._values, dict):
            self._values[field_or_dict] = (value, operator)
        else:
            self._values.append((field_or_dict, (value, operator)))

        return self

    def where_raw_value(
        self,
        field_or_dict: str | Mapping[str, WhereRawValueT],
        raw_value: WhereRawValueT | None = None,
        operator: WhereOpT = "=",
        value_params: StmtParamValuesT | None = None,
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
            raw_value (string, optional): Value to compare with if ``field_or_dict`` is a field name.
            operator (string, optional): Comparison operator, default is '='.
            value_params (Sequence, optional): List of value params. Default is None.

        Returns:
            object: self
        """
        assert isinstance(field_or_dict, (str, dict))
        assert raw_value is None or isinstance(raw_value, (str, tuple))
        assert isinstance(operator, str)
        assert value_params is None or isinstance(value_params, Sequence)

        if isinstance(field_or_dict, Mapping):
            for f, v in field_or_dict.items():
                self.where_raw_value(f, v)
        elif raw_value is None:
            errmsg = "Raw value cannot be 'None'"
            raise ValueError(errmsg)
        elif isinstance(self._values_raw, dict):
            self._values_raw[field_or_dict] = (raw_value, operator, value_params)
        else:
            self._values_raw.append((field_or_dict, (raw_value, operator, value_params)))

        return self

    def where_expr(
        self,
        expr_or_list: WhereExprT,
        expr_params: ValueParamsT | None = None,
    ) -> WhereCondition:
        """Include a complex expression in conditional statement.

        Expressions will be included in the SQL statement verbatim.
        Use :py:meth:`where_value` or :py:meth:`where_raw_value` if you are
        doing basic field comparisons and/or want the value to be pickled.

        Arguments:
            expr_or_list (string or list): An expression or :py:class:`list` of expressions.
            expr_params (Sequence, optional): List of expression params. Default is None.

        Returns:
            object: self
        """
        assert expr_params is None or isinstance(expr_params, Sequence)

        if not isinstance(expr_or_list, str):
            for expr in expr_or_list:
                self.where_expr(expr)
        else:
            self._raw_exprs.append((expr_or_list, expr_params))

        return self

    def where_select(
        self,
        field: str,
        stmt: Stmt,
        operator: WhereOpT,
        value_params: StmtParamValuesT | None = None,
    ) -> WhereCondition:
        """Test field membership in subquery result.

        Field names may be escaped with backticks.

        Parameterized values will be pickled by :py:meth:`mysqlstmt.stmt.Stmt.pickle`.

        Arguments:
            field (string): Name of field/column .
            stmt (Select): SELECT subquery.
            operator (string): Membership operator [NOT] IN or [NOT] EXISTS.
            value_params (Sequence, optional): List of value params for SELECT. Default is None.

        Returns:
            object: self
        """
        assert isinstance(field, str)
        assert isinstance(stmt, Stmt)
        assert isinstance(operator, str)
        assert value_params is None or isinstance(value_params, Sequence)

        if isinstance(self._selects, dict):
            self._selects[field] = (stmt, operator, value_params)
        else:
            self._selects.append((field, (stmt, operator, value_params)))

        return self

    def sql(self, param_values: list[str]) -> str | None:  # noqa: C901, PLR0912, PLR0915
        """Build SQL snippet to include in a WHERE or HAVING clause.

        Args:
            param_values (list, modified): List to append parameterized values to.

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
            if not isinstance(field_or_tuple, str):
                # > OR = tuple
                field, value_op_tuple = field_or_tuple
                val, op = value_op_tuple
            elif isinstance(self._values, dict):
                # > AND = dict key
                val, op = self._values[field_or_tuple]
                field = field_or_tuple
            else:
                errmsg = "WhereCondition expected a tuple or string dictionary key"
                raise TypeError(errmsg)

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
            if not isinstance(field_or_tuple, str):
                field, value_op_tuple = field_or_tuple
                val, op, val_params = value_op_tuple
            elif isinstance(self._values_raw, dict):
                val, op, val_params = self._values_raw[field_or_tuple]
                field = field_or_tuple
            else:
                errmsg = "WhereCondition expected a tuple or string dictionary key"
                raise TypeError(errmsg)

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

        for field_or_tuple in self._selects:
            if not isinstance(field_or_tuple, str):
                field, value_op_tuple = field_or_tuple
                stmt, op, val_params = value_op_tuple
            elif isinstance(self._selects, dict):
                stmt, op, val_params = self._selects[field_or_tuple]
                field = field_or_tuple
            else:
                errmsg = "WhereCondition expected a tuple or string dictionary key"
                raise TypeError(errmsg)

            if val_params is not None and self._stmt.placeholder:
                for param_val in val_params:
                    pickled_val, can_paramize_val = self._stmt.pickle(param_val)
                    param_values.append(pickled_val)

            select_sql, select_params = stmt.sql() if stmt.placeholder else (str(stmt.sql()), None)

            if select_params is not None:
                param_values.extend(select_params)

            sql.append(f"{self._stmt.quote_col_ref(field)} {op} ({select_sql})")

        if not sql:
            return None

        sql = self._predicate.join(sql)

        if self._negate:
            sql = f"NOT ({sql})"
        elif self.expr_count > 1:
            sql = f"({sql})"

        return sql
