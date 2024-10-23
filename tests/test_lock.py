import pytest

from mysqlstmt import Lock


class TestLock:
    def test_unnamed_params(self) -> None:
        q = Lock("mylock", 5)
        sql_t = q.sql()
        assert sql_t == ("SELECT GET_LOCK('mylock', 5)", None)

    def test_named_params(self) -> None:
        q = Lock(name="mylock", timeout=5)
        sql_t = q.sql()
        assert sql_t == ("SELECT GET_LOCK('mylock', 5)", None)

    def test_release(self) -> None:
        q = Lock("mylock", 5)
        sql_t = q.release_lock()
        assert sql_t == ("SELECT RELEASE_LOCK('mylock')", None)

    def test_free(self) -> None:
        q = Lock("mylock", 5)
        sql_t = q.is_free_lock()
        assert sql_t == ("SELECT IS_FREE_LOCK('mylock')", None)

    def test_fail_no_name(self) -> None:
        q = Lock()
        with pytest.raises(ValueError):
            q.sql()

    def test_fail_no_timeout(self) -> None:
        q = Lock("mylock")
        with pytest.raises(ValueError):
            q.sql()
