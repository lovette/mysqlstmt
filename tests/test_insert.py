from collections import OrderedDict

import pytest

from mysqlstmt import Insert, Select


class TestInsert:
    def test_constructor_table_name(self) -> None:
        q = Insert("t1")
        sql_t = q.set_value("t1c1", 1).sql()
        assert sql_t == ("INSERT INTO t1 (`t1c1`) VALUES (1)", None)

    def test_set_value_int(self) -> None:
        q = Insert()
        sql_t = q.into_table("t1").set_value("t1c1", 1).sql()
        assert sql_t == ("INSERT INTO t1 (`t1c1`) VALUES (1)", None)

    def test_set_value_int_callable(self) -> None:
        q = Insert()
        sql_t = q.into_table("t1").set_value("t1c1", 1)()
        assert sql_t == ("INSERT INTO t1 (`t1c1`) VALUES (1)", None)

    def test_set_value_ints(self) -> None:
        q = Insert()
        sql_t = q.into_table("t1").set_value("t1c1", 1).set_value("t1c2", 2).sql()
        assert sql_t == ("INSERT INTO t1 (`t1c1`, `t1c2`) VALUES (1, 2)", None)

    def test_dict_int(self) -> None:
        q = Insert()
        values = {"t1c1": 1}
        sql_t = q.into_table("t1").set_value(values).sql()
        assert sql_t == ("INSERT INTO t1 (`t1c1`) VALUES (1)", None)

    def test_dict_ints(self) -> None:
        q = Insert()
        values = {**OrderedDict([("t1c1", 1), ("t1c2", 2)])}
        sql_t = q.into_table("t1").set_value(values).sql()
        assert sql_t == ("INSERT INTO t1 (`t1c1`, `t1c2`) VALUES (1, 2)", None)

    def test_dict_strings(self) -> None:
        q = Insert()
        values = {**OrderedDict([("t1c1", "a"), ("t1c2", "b")])}
        sql_t = q.into_table("t1").set_value(values).sql()
        assert sql_t == ("INSERT INTO t1 (`t1c1`, `t1c2`) VALUES (?, ?)", ("a", "b"))

    def test_null(self) -> None:
        q = Insert()
        values = {**OrderedDict([("t1c1", "a"), ("t1c2", None)])}
        sql_t = q.into_table("t1").set_value(values).sql()
        assert sql_t == ("INSERT INTO t1 (`t1c1`, `t1c2`) VALUES (?, NULL)", ("a",))

    def test_function_value(self) -> None:
        q = Insert()
        sql_t = q.into_table("t1").set_value("t1c1", "NOW()").sql()
        assert sql_t == ("INSERT INTO t1 (`t1c1`) VALUES (?)", ("NOW()",))

    def test_function_raw_value(self) -> None:
        q = Insert()
        sql_t = q.into_table("t1").set_raw_value("t1c1", "NOW()").sql()
        assert sql_t == ("INSERT INTO t1 (`t1c1`) VALUES (NOW())", None)

    def test_function_raw_value_dict(self) -> None:
        q = Insert()
        sql_t = q.into_table("t1").set_raw_value({"t1c1": "NOW()"}).sql()
        assert sql_t == ("INSERT INTO t1 (`t1c1`) VALUES (NOW())", None)

    def test_function_raw_value_with_valparams(self) -> None:
        q = Insert()
        sql_t = q.into_table("t1").set_raw_value("t1c1", "PASSWORD(?)", value_params=("mypw",)).sql()
        assert sql_t == ("INSERT INTO t1 (`t1c1`) VALUES (PASSWORD(?))", ("mypw",))

    def test_function_raw_value_dict_with_valparams(self) -> None:
        q = Insert()
        sql_t = q.into_table("t1").set_raw_value({"t1c1": ("PASSWORD(?)", ("mypw",))}).sql()
        assert sql_t == ("INSERT INTO t1 (`t1c1`) VALUES (PASSWORD(?))", ("mypw",))

    def test_select_string_col(self) -> None:
        q = Insert()
        sql_t = q.into_table("t1").columns("t1c1").select("SELECT t2c1 FROM t2").sql()
        assert sql_t == ("INSERT INTO t1 (`t1c1`) SELECT t2c1 FROM t2", None)

    def test_select_string_cols(self) -> None:
        q = Insert()
        sql_t = q.into_table("t1").columns(["t1c1", "t1c2"]).select("SELECT `t2c1`, `t2c2` FROM t2").sql()
        assert sql_t == ("INSERT INTO t1 (`t1c1`, `t1c2`) SELECT `t2c1`, `t2c2` FROM t2", None)

    def test_select_obj_cols(self) -> None:
        q = Insert()
        qselect = Select("t2").columns(["t2c1", "t2c2"])
        sql_t = q.into_table("t1").columns(["t1c1", "t1c2"]).select(qselect).sql()
        assert sql_t == ("INSERT INTO t1 (`t1c1`, `t1c2`) SELECT `t2c1`, `t2c2` FROM t2", None)

    def test_ignore(self) -> None:
        q = Insert("t1", ignore_error=True)
        sql_t = q.set_value("t1c1", 1).sql()
        assert sql_t == ("INSERT IGNORE INTO t1 (`t1c1`) VALUES (1)", None)

    def test_function_batch_1x1(self) -> None:
        q = Insert()
        data = (["v1"],)
        sql_t = q.into_table("t1").columns("t1c1").set_batch_value(data).sql()
        assert sql_t == ("INSERT INTO t1 (`t1c1`) VALUES (?)", data)

    def test_function_batch_3x1(self) -> None:
        q = Insert()
        data = (["v1"], ["v2"], ["NOW()"])
        sql_t = q.into_table("t1").columns("t1c1").set_batch_value(data).sql()
        assert sql_t == ("INSERT INTO t1 (`t1c1`) VALUES (?)", data)

    def test_function_batch_3x3(self) -> None:
        q = Insert()
        data = (["v1", "v2", "NOW()"], ["v1", "v2", "NOW()"], ["v1", "v2", "NOW()"])
        sql_t = q.into_table("t1").columns(["t1c1", "t1c2", "t1c3"]).set_batch_value(data).sql()
        assert sql_t == ("INSERT INTO t1 (`t1c1`, `t1c2`, `t1c3`) VALUES (?, ?, ?)", data)

    def test_function_batch_1x1_noparam(self) -> None:
        q = Insert(placeholder=False)
        data = (["'v1'"],)
        sql = q.into_table("t1").columns("t1c1").set_batch_value(data).sql()
        assert sql == "INSERT INTO t1 (`t1c1`) VALUES ('v1')"

    def test_function_batch_3x1_noparam(self) -> None:
        q = Insert(placeholder=False)
        data = (["'v1'"], ["'v2'"], ["NOW()"])
        sql = q.into_table("t1").columns("t1c1").set_batch_value(data).sql()
        assert sql == "INSERT INTO t1 (`t1c1`) VALUES ('v1'), ('v2'), (NOW())"

    def test_function_batch_3x3_noparam(self) -> None:
        q = Insert(placeholder=False)
        data = (["'r1v1'", "'r1v2'", "NOW()"], ["'r2v1'", "'r2v2'", "NOW()"], ["'r3v1'", "'r3v2'", "NOW()"])
        sql = q.into_table("t1").columns(["t1c1", "t1c2", "t1c3"]).set_batch_value(data).sql()
        assert sql == "INSERT INTO t1 (`t1c1`, `t1c2`, `t1c3`) VALUES ('r1v1', 'r1v2', NOW()), ('r2v1', 'r2v2', NOW()), ('r3v1', 'r3v2', NOW())"

    def test_dict_strings_utf_param(self) -> None:
        q = Insert()
        values = {**OrderedDict([("t1c1", "äöü")])}
        sql_t = q.into_table("t1").set_value(values).sql()
        assert sql_t == ("INSERT INTO t1 (`t1c1`) VALUES (?)", ("äöü",))

    def test_dict_strings_utf_raw(self) -> None:
        q = Insert()
        sql_t = q.into_table("t1").set_raw_value("t1c1", '"äöü"').sql()
        assert sql_t == ('INSERT INTO t1 (`t1c1`) VALUES ("äöü")', None)

    def test_dict_strings_utf_batch(self) -> None:
        q = Insert()
        data = (["äöü"],)
        sql_t = q.into_table("t1").columns("t1c1").set_batch_value(data).sql()
        assert sql_t == ("INSERT INTO t1 (`t1c1`) VALUES (?)", data)

    def test_dict_strings_utf_noparam(self) -> None:
        q = Insert(placeholder=False)
        sql = q.into_table("t1").set_value("t1c1", '"äöü"').sql()
        assert sql == 'INSERT INTO t1 (`t1c1`) VALUES ("äöü")'

    def test_set_value_int_option(self) -> None:
        q = Insert()
        sql_t = q.set_option("LOW_PRIORITY").into_table("t1").set_value("t1c1", 1).sql()
        assert sql_t == ("INSERT LOW_PRIORITY INTO t1 (`t1c1`) VALUES (1)", None)

    def test_fail_no_tables(self) -> None:
        q = Insert()
        with pytest.raises(ValueError):  # noqa: PT011
            q.set_value("t1c1", 1).sql()

    def test_fail_multi_tables(self) -> None:
        with pytest.raises(ValueError):  # noqa: PT011
            Insert(["t1", "t2"])  # pyright: ignore[reportArgumentType]

    def test_fail_no_values(self) -> None:
        q = Insert("t1")
        with pytest.raises(ValueError):  # noqa: PT011
            q.sql()

    def test_fail_set_columns(self) -> None:
        q = Insert()
        with pytest.raises(ValueError):  # noqa: PT011
            q.into_table("t1").columns("t1c1").set_value("t1c1", 1).sql()

    def test_fail_select_with_set_value(self) -> None:
        q = Insert()
        with pytest.raises(ValueError):  # noqa: PT011
            q.into_table("t1").set_value("t1c1", 1).select("SELECT * FROM t2").sql()

    def test_fail_select_no_columns(self) -> None:
        q = Insert()
        with pytest.raises(ValueError):  # noqa: PT011
            q.into_table("t1").select("SELECT * FROM t2").sql()

    def test_fail_batch_values(self) -> None:
        q = Insert()
        data = [["v1"]]
        with pytest.raises(ValueError):  # noqa: PT011
            q.into_table("t1").set_value("t1c1", 1).set_batch_value(data).sql()

    def test_fail_batch_no_columns(self) -> None:
        q = Insert()
        data = [["v1"]]
        with pytest.raises(ValueError):  # noqa: PT011
            q.into_table("t1").set_batch_value(data).sql()

    def test_fail_batch_select(self) -> None:
        q = Insert()
        data = [["v1"]]
        with pytest.raises(ValueError):  # noqa: PT011
            q.into_table("t1").columns("t1c1").set_batch_value(data).select("SELECT * FROM t2").sql()

    def test_fail_select_with_params(self) -> None:
        q = Insert()
        qselect = Select("t2").columns(["t2c1"]).where_value("t2c1", "t2v1")
        with pytest.raises(ValueError):  # noqa: PT011
            q.into_table("t1").columns(["t1c1"]).select(qselect).sql()

    def test_allow_select_with_params(self) -> None:
        q = Insert(select_allow_placeholders=True)
        qselect = Select("t2").columns(["t2c1"]).where_value("t2c1", "t2v1")
        sql_t = q.into_table("t1").columns(["t1c1"]).select(qselect).sql()
        assert sql_t == ("INSERT INTO t1 (`t1c1`) SELECT `t2c1` FROM t2 WHERE `t2c1` = ?", ("t2v1",))
