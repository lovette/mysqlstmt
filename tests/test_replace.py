from mysqlstmt import Replace


class TestReplace:
    def test_set_value_int(self):
        q = Replace()
        sql_t = q.into_table("t").set_value("c1", 1).sql()
        assert sql_t == ("REPLACE INTO t (`c1`) VALUES (1)", None)
