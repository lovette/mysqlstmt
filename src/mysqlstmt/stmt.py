import datetime
import collections
from .config import Config


class Stmt:
    """Base class for all statement classes.
    """

    def __init__(self, placeholder=None, quote_all_col_refs=None, quote_all_values=None, **kwargs):
        """Constructor

        Keyword Arguments:
            placeholder (string, optional): Placeholder character to use when parameterization is enabled.
                Default is None, in which case the :py:class:`mysqlstmt.config.Config` setting will be used.
            quote_all_col_refs (bool, optional): Quote all column references.
                Default is None, in which case the :py:class:`mysqlstmt.config.Config` setting will be used.
            quote_all_values (bool, optional): The predicate for the outer WHERE condition, either 'AND' or 'OR'.
                Default is None, in which case the :py:class:`mysqlstmt.config.Config` setting will be used.
            **kwargs: Base class arguments.

        Note:
            Default settings for ``placeholder``, ``quote_all_col_refs`` and ``quote_all_values``
            are set through :py:class:`mysqlstmt.config.Config`
        """
        super().__init__(**kwargs)

        if placeholder is False or Config.placeholder is False:
            self.placeholder = False
        else:
            self.placeholder = Config.placeholder if placeholder is None else placeholder

        if quote_all_values is False or Config.quote_all_values is False:
            self.quote_all_values = False
        else:
            self.quote_all_values = Config.quote_all_values if quote_all_values is None else quote_all_values

        if quote_all_col_refs is False or Config.quote_all_col_refs is False:
            self.quote_all_col_refs = False
        else:
            self.quote_all_col_refs = Config.quote_all_col_refs if quote_all_col_refs is None else quote_all_col_refs

        # Public properties
        self.query_options = []  # can append with ``set_option``

    def __call__(self, *args, **kwargs):
        """Returns SQL statement created by :py:meth:`sql`"""
        return self.sql()

    def __str__(self):
        """Returns SQL statement created by :py:meth:`sql`"""
        sql_t = self.sql()
        return sql_t[0] if self.placeholder else sql_t

    def sql(self):
        """Derived classes must override and build appropriate SQL statement.

        Returns:
            Either a tuple ``(SQL statement, parameterized values)`` if ``placeholder`` is set,
            otherwise SQL statement as string.

        Raises:
            ValueError: The statement cannot be created with the given attributes.
            NotImplementedError: There is no base class implementation.
        """
        raise NotImplementedError

    def quote_col_ref(self, col_ref):
        """Quote column reference with backticks.

        Arguments:
            col_ref (string): Column reference. Can be prefixed with the table name.

        Returns:
            string: Column reference quoted with backticks (``).

        Notes:
            Column reference will not be quoted if it contains a backtick, space or parenthesis.
        """
        if self.quote_all_col_refs:
            if ' ' in col_ref:
                return col_ref  # COLUMN AS ALIAS
            if '(' in col_ref:
                return col_ref  # FUNCTION(COLUMN)
            if '`' in col_ref:
                return col_ref  # already quoted

            col_ref_parts = col_ref.split('.')
            if len(col_ref_parts) > 1:
                table, col = col_ref_parts
                return f'{table}.`{col}`'
            else:
                return f'`{col_ref}`'

        return col_ref

    def pickle(self, val):
        """Convert variable value into a value that can be included in a SQL statement.

        Arguments:
            val (mixed): Value to pickle.

        Returns:
            tuple: (string, bool) Pickled value as a string and True if value should be parameterized.
        """
        if val is None:
            return 'NULL', False
        elif val is True:
            return '1', False
        elif val is False:
            return '0', False
        elif isinstance(val, str):
            return val, True
        elif isinstance(val, (int, long, float)):
            return str(val), False
        elif isinstance(val, datetime.datetime):
            return val.strftime('%Y-%m-%d %H:%M:%S'), True
        elif isinstance(val, datetime.date):
            return val.strftime('%Y-%m-%d'), True
        elif isinstance(val, datetime.time):
            return val.strftime('%H:%M:%S'), True
        return unicode(val), True

    @staticmethod
    def quote(val):
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
        return "'{0}'".format(val.replace("'", "\\'"))

    @staticmethod
    def table_alias(table_factor):
        """Returns the table alias from a table factor.

        Arguments:
            table_factor (string): Table factor reference such as ``table`` or ``table AS alias``.

        Returns:
            string
        """
        table_parts = table_factor.split('AS')
        return table_factor if len(table_parts) == 1 else table_parts[1].strip()

    def _parameterize_values(self, list_or_value, inline_values, param_values):
        """Parameterizes a value or list of values.

        Evaluates or iterates through ``list_or_value`` and if the value can be parameterized
        it is added to ``param_values``, otherwise it is added to ``inline_values``.

        Arguments:
            list_or_value (list or mixed): A value or list of values to replace with ``placeholder``.
            inline_values (list or None): List to append non-parameterized values to;
                set to None to force everything to be parameterized.
            param_values (list or None): List to append parameterized values to;
                set to None to force everything not to be inlined.
        """
        if isinstance(list_or_value, collections.abc.Iterable) and not isinstance(list_or_value, str):
            for val in list_or_value:
                self._parameterize_values(val, inline_values, param_values)
        else:
            using_placeholder = False if (param_values is None) else bool(self.placeholder)
            quote = False if using_placeholder is True else self.quote_all_values

            list_or_value, can_paramize_val = self.pickle(list_or_value)

            if inline_values is None:
                param_values.append(list_or_value)
            elif can_paramize_val and using_placeholder:
                inline_values.append(self.placeholder)
                param_values.append(list_or_value)
            elif can_paramize_val and quote:
                inline_values.append(self.quote(list_or_value))
            else:
                inline_values.append(list_or_value)

    def set_option(self, list_or_value):
        """Sets query options (the keywords at the beginning of the SQL statement).

        Arguments:
            list_or_value (list or mixed): An option or list of options.

        Returns:
            object: self
        """
        if isinstance(list_or_value, collections.abc.Iterable) and not isinstance(list_or_value, str):
            for val in list_or_value:
                self.set_option(val)
        else:
            self.query_options.append(list_or_value)

        return self
