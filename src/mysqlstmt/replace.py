import mysqlstmt


class Replace(mysqlstmt.Insert):
    """REPLACE statement.

    Examples: ::

        >>> q = Replace()
        >>> q.into_table('t').set_value('c1', 1).sql()
        ('REPLACE INTO t (`c1`) VALUES (1)', None)
    """

    def __init__(self, **kwargs):
        """Constructor for Replace

        Keyword Arguments:
            **kwargs: Base class arguments.
        """
        super(Replace, self).__init__(**kwargs)

        self._replace = True
