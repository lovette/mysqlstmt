"""mysqlstmt set_values_mixin class module.

This module provides:
- SetValuesMixin
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING
from typing import Union as UnionT

from .stmt import StmtParamValuesT, StmtPickleT

if TYPE_CHECKING:
    from typing_extensions import Self


SetValueT = StmtPickleT
SetRawValueTupleT = tuple[str, UnionT[StmtParamValuesT, None]]
SetRawValueT = UnionT[str, SetRawValueTupleT]


class SetValuesMixin:
    """Provide set_value related functionality to statement classes.

    Note:
        This class is not to be instantiated directly.
    """

    def __init__(self, **kwargs) -> None:
        """Constructor.

        Keyword Arguments:
            **kwargs: Base class arguments.
        """
        super().__init__(**kwargs)

        self._values: dict[str, SetValueT] = {}
        self._values_raw: dict[str, SetRawValueTupleT] = {}

    def set_value(self, field_or_dict: str | Mapping[str, SetValueT], value: SetValueT = None) -> Self:
        """Set value that may be translated, escaped or parameterized.

        Field names may be escaped with backticks.
        Values will be pickled by :py:meth:`mysqlstmt.stmt.Stmt.pickle`.
        Use :py:meth:`set_raw_value` if you want values to be
        included in the SQL statement verbatim.

        Arguments:
            field_or_dict (string or dict): Field name or :py:class:`dict` mapping fields to values.
            value (string, optional): Field value if ``field_or_dict`` is a field name.

        Returns:
            object: self

        Examples: ::

            >>> q = Insert()
            >>> q.into_table('t1').set_value('t1c1', 1).sql()
            ('INSERT INTO t1 (`t1c1`) VALUES (1)', None)

            >>> q = Insert()
            >>> q.into_table('t1').set_value('t1c1', 1)()
            ('INSERT INTO t1 (`t1c1`) VALUES (1)', None)

            >>> q = Insert()
            >>> q.into_table('t1').set_value('t1c1', 1).set_value('t1c2', 2).sql()
            ('INSERT INTO t1 (`t1c1`, `t1c2`) VALUES (1, 2)', None)

            >>> q = Insert()
            >>> values = {'t1c1': 1}
            >>> q.into_table('t1').set_value(values).sql()
            ('INSERT INTO t1 (`t1c1`) VALUES (1)', None)

            >>> q = Insert()
            >>> values = OrderedDict([('t1c1',1), ('t1c2',2)])
            >>> q.into_table('t1').set_value(values).sql()
            ('INSERT INTO t1 (`t1c1`, `t1c2`) VALUES (1, 2)', None)

            >>> q = Insert()
            >>> values = OrderedDict([('t1c1','a'), ('t1c2','b')])
            >>> q.into_table('t1').set_value(values).sql()
            ("INSERT INTO t1 (`t1c1`, `t1c2`) VALUES (?, ?)", ['a','b'])

            >>> q = Insert()
            >>> values = OrderedDict([('t1c1','a'), ('t1c2',None)])
            >>> q.into_table('t1').set_value(values).sql()
            ("INSERT INTO t1 (`t1c1`, `t1c2`) VALUES (?, NULL)", ['a'])

            >>> q = Insert()
            >>> q.into_table('t1').set_value('t1c1', 'NOW()').sql()
            ('INSERT INTO t1 (`t1c1`) VALUES (?)', ['NOW()'])
        """
        if isinstance(field_or_dict, Mapping):
            for f, v in field_or_dict.items():
                self.set_value(f, v)
        else:
            self._values[field_or_dict] = value

        return self

    set_values = set_value
    """Alias for :py:meth:`set_value`"""

    def set_raw_value(
        self,
        field_or_dict: str | Mapping[str, SetRawValueT],
        value_or_tuple: SetRawValueT | None = None,
        value_params: StmtParamValuesT | None = None,
    ) -> Self:
        """Set value to be included directly in the SQL.

        Field names may be escaped with backticks.
        Values will be included in the SQL statement verbatim.
        Use :py:meth:`set_value` if you want values to be pickled.

        Arguments:
            field_or_dict (string or dict): Field name or :py:class:`dict` mapping fields to values.
                Dictionary values can also be a tuple, as described below.
            value_or_tuple (string, optional): Field value if ``field_or_dict`` is a field name.
                Can also be a tuple (value, value_params).
            value_params (collection, optional): List of value params. Default is None.

        Returns:
            object: self

        Examples: ::

            >>> q = Insert()
            >>> q.into_table('t1').set_raw_value('t1c1', 'NOW()').sql()
            ('INSERT INTO t1 (`t1c1`) VALUES (NOW())', None)

            >>> q = Insert()
            >>> q.into_table('t1').set_raw_value({'t1c1':'NOW()'}).sql()
            ('INSERT INTO t1 (`t1c1`) VALUES (NOW())', None)

            >>> q = Insert()
            >>> q.into_table('t1').set_raw_value('t1c1', 'PASSWORD(?)', value_params=('mypw',)).sql()
            ('INSERT INTO t1 (`t1c1`) VALUES (PASSWORD(?))', ['mypw'])
        """
        assert isinstance(field_or_dict, (str, Mapping))
        assert value_or_tuple is None or isinstance(value_or_tuple, (str, tuple))
        assert value_params is None or isinstance(value_params, Sequence)

        if isinstance(field_or_dict, Mapping):
            for f, v in field_or_dict.items():
                self.set_raw_value(f, v)
        elif value_or_tuple is None:
            errmsg = "Raw value cannot be 'None'"
            raise ValueError(errmsg)
        elif isinstance(value_or_tuple, tuple):
            assert len(value_or_tuple) == 2  # noqa: PLR2004
            self._values_raw[field_or_dict] = value_or_tuple
        else:
            self.set_raw_value(field_or_dict, (value_or_tuple, value_params))

        return self

    set_raw_values = set_raw_value
    """Alias for :py:meth:`set_raw_value`"""
