# mysqlstmt

Python package to build SQL statements using objects.

- Some query options may generate SQL specific to MySQL but in general queries should work for any SQL database.
- Supports parameterized queries using a placeholder character.

## Examples

	>>> from mysqlstmt import Select, Insert
	>>> data = [["'v1'"], ["'v2'"], ["NOW()"]]

	>>> Select("t1").columns("t1c1").sql()
	('SELECT `t1c1` FROM t1', None)

	>>> Select("t1", placeholder=False).columns("t1c1").sql()
	'SELECT `t1c1` FROM t1'

	>>> Select().columns(["t1c1", "t2c1"]).from_table("t1").left_join("t2", "t1c1").sql()
	('SELECT `t1c1`, `t2c1` FROM t1 LEFT JOIN t2 USING (`t1c1`)', None)

	>>> Insert("t1").set_value("t1c1", 1).sql()
	('INSERT INTO t1 (`t1c1`) VALUES (1)', None)

	>>> Insert().into_table("t1").columns("t1c1").set_batch_value(data).sql()
	('INSERT INTO t1 (`t1c1`) VALUES (?)', [["'v1'"], ["'v2'"], ['NOW()']])

	>>> Insert(placeholder=False).into_table("t1").columns("t1c1").set_batch_value(data).sql()
	"INSERT INTO t1 (`t1c1`) VALUES ('v1'), ('v2'), (NOW())"

## Install for development

	git clone https://github.com/lovette/mysqlstmt.git
	cd mysqlstmt/
	make virtualenv
	source $HOME/.virtualenvs/mysqlstmt/bin/activate
	make requirements
	make install-dev
