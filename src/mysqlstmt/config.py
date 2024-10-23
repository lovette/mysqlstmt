"""mysqlstmt config class module.

This module provides:
- Config
"""


class Config:
    """Global configuration variables for all queries derived from ``mysqlstmt.Stmt``.

    Values set on these class attributes will affect all queries.

    Examples: ::

        mysqlstmt.Config.placeholder = '%'

    """

    placeholder = "?"
    """Parameterize queries using this placeholder.

    * None = Per class instance
    * String = Placeholder value to use unless instance overrides
    * False = Disable parameterized queries
    """

    quote_all_values = False
    """Call :py:meth:`mysqlstmt.stmt.Stmt.quote` for non-parameterized string values.

    * None = Per class instance
    * True = Always
    * False = Never
    """

    quote_all_col_refs = True
    """Quote all column references with backticks.

    * None = Per class instance
    * True = Always
    * False = Never
    """

    select_cacheable = None
    """Whether MySQL should cache SELECT results.

    * None = Per class instance
    * True = Always
    * False = Never
    """
