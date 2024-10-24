"""mysqlstmt lock class module.

This module provides:
- Lock
"""

from __future__ import annotations

from .stmt import Stmt


class Lock(Stmt):
    """SELECT GET_LOCK statement."""

    def __init__(self, name: str | None = None, timeout: int | None = None, **kwargs) -> None:
        """Constructor.

        Keyword Arguments:
            name (string): Name of lock.
            timeout (int): Lock timeout, in seconds.
            **kwargs: Base class arguments.
        """
        super().__init__(**kwargs)

        self._name = name
        self._timeout = timeout

    def get_lock(self) -> str:
        """Build SELECT GET_LOCK SQL statement.

        Returns:
            Either a tuple ``(SQL statement, None)`` if ``placeholder`` is set,
            otherwise SQL statement as string.

        Raises:
            ValueError: The statement cannot be created with the given attributes.

        Examples: ::

            >>> q = Lock('mylock', 5)
            >>> sql_t = q.sql()
            ("SELECT GET_LOCK('mylock', 5)", None)

            >>> q = Lock(name='mylock', timeout=5)
            >>> sql_t = q.sql()
            ("SELECT GET_LOCK('mylock', 5)", None)
        """
        if not self._name:
            msg = "Lock name is required"
            raise ValueError(msg)
        if not self._timeout:
            msg = "Lock timeout is required"
            raise ValueError(msg)

        sql = ["SELECT", f"GET_LOCK({self.quote(self._name)}, {self._timeout})"]

        if self.placeholder:
            return " ".join(sql), None
        return " ".join(sql)

    def release_lock(self) -> str:
        """Build SELECT RELEASE_LOCK SQL statement.

        Returns:
            Either a tuple ``(SQL statement, None)`` if ``placeholder`` is set,
            otherwise SQL statement as string.

        Raises:
            ValueError: The statement cannot be created with the given attributes.

        Examples: ::

            >>> q = Lock('mylock', 5)
            >>> sql_t = q.release_lock()
            ("SELECT RELEASE_LOCK('mylock')", None)
        """
        if not self._name:
            msg = "Lock name is required"
            raise ValueError(msg)

        sql = ["SELECT", f"RELEASE_LOCK({self.quote(self._name)})"]

        if self.placeholder:
            return " ".join(sql), None
        return " ".join(sql)

    def is_free_lock(self) -> str:
        """Build SELECT IS_FREE_LOCK SQL statement.

        Returns:
            Either a tuple ``(SQL statement, None)`` if ``placeholder`` is set,
            otherwise SQL statement as string.

        Raises:
            ValueError: The statement cannot be created with the given attributes.

        Examples: ::

            >>> q = Lock('mylock', 5)
            >>> sql_t = q.is_free_lock()
            ("SELECT IS_FREE_LOCK('mylock')", None)
        """
        if not self._name:
            msg = "Lock name is required"
            raise ValueError(msg)

        sql = ["SELECT", f"IS_FREE_LOCK({self.quote(self._name)})"]

        if self.placeholder:
            return " ".join(sql), None
        return " ".join(sql)

    sql = get_lock
    """Alias for :py:meth:`get_lock`."""
