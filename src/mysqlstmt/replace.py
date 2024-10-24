"""mysqlstmt replace class module.

This module provides:
- Replace
"""

from .insert import Insert


class Replace(Insert):
    """REPLACE statement.

    Examples: ::

        >>> q = Replace()
        >>> q.into_table('t').set_value('c1', 1).sql()
        ('REPLACE INTO t (`c1`) VALUES (1)', None)
    """

    def __init__(self, **kwargs) -> None:
        """Constructor.

        Keyword Arguments:
            **kwargs: Base class arguments.
        """
        super().__init__(**kwargs)

        self._replace = True
