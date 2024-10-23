import pytest

from mysqlstmt import Select, Union


class TestUnion:
    def test_single(self) -> None:
        q = Union()
        sql_t = q.union("SELECT `t1c1` FROM t1").sql()
        assert sql_t == ("(SELECT `t1c1` FROM t1)", None)

    def test_double(self) -> None:
        q = Union()
        sql_t = q.union("SELECT `t1c1` FROM t1").select("SELECT `t2c1` FROM t2").sql()
        assert sql_t == ("(SELECT `t1c1` FROM t1) UNION (SELECT `t2c1` FROM t2)", None)

    def test_orderby(self) -> None:
        q = Union()
        sql_t = q.union("SELECT `t1c1` AS sort_col FROM t1").select("SELECT `t2c1` FROM t2").order_by("sort_col, DESC").sql()
        assert sql_t == ("(SELECT `t1c1` AS sort_col FROM t1) UNION (SELECT `t2c1` FROM t2) ORDER BY sort_col, DESC", None)

    def test_orderby_limit(self) -> None:
        q = Union()
        sql_t = q.union("SELECT `t1c1` AS sort_col FROM t1").select("SELECT `t2c1` FROM t2").order_by("sort_col, DESC").limit(5).sql()
        assert sql_t == ("(SELECT `t1c1` AS sort_col FROM t1) UNION (SELECT `t2c1` FROM t2) ORDER BY sort_col, DESC LIMIT 5", None)

    def test_orderby_limit_offset(self) -> None:
        q = Union()
        sql_t = q.union("SELECT `t1c1` AS sort_col FROM t1").select("SELECT `t2c1` FROM t2").order_by("sort_col, DESC").limit(10, 5).sql()
        assert sql_t == ("(SELECT `t1c1` AS sort_col FROM t1) UNION (SELECT `t2c1` FROM t2) ORDER BY sort_col, DESC LIMIT 5,10", None)

    def test_single_obj(self) -> None:
        s = Select("t1").column("t1c1")
        q = Union()
        sql_t = q.union(s).sql()
        assert sql_t == ("(SELECT `t1c1` FROM t1)", None)

    def test_double_obj(self) -> None:
        s1 = Select("t1").column("t1c1")
        s2 = Select("t2").column("t2c1")
        q = Union()
        sql_t = q.union(s1).union(s2).sql()
        assert sql_t == ("(SELECT `t1c1` FROM t1) UNION (SELECT `t2c1` FROM t2)", None)

    def test_double_obj_list(self) -> None:
        s1 = Select("t1").column("t1c1")
        s2 = Select("t2").column("t2c1")
        q = Union()
        sql_t = q.union([s1, s2]).sql()
        assert sql_t == ("(SELECT `t1c1` FROM t1) UNION (SELECT `t2c1` FROM t2)", None)

    def test_double_obj_list_constructor(self) -> None:
        s1 = Select("t1").column("t1c1")
        s2 = Select("t2").column("t2c1")
        q = Union([s1, s2])
        sql_t = q.sql()
        assert sql_t == ("(SELECT `t1c1` FROM t1) UNION (SELECT `t2c1` FROM t2)", None)

    def test_union_all(self) -> None:
        q = Union(distinct=False)
        sql_t = q.union("SELECT `t1c1` FROM t1").select("SELECT `t2c1` FROM t2").sql()
        assert sql_t == ("(SELECT `t1c1` FROM t1) UNION ALL (SELECT `t2c1` FROM t2)", None)

    def test_fail_no_stmts(self) -> None:
        q = Union()
        with pytest.raises(ValueError):
            q.sql()
