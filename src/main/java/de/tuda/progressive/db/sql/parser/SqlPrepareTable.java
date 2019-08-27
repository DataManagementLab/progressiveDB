package de.tuda.progressive.db.sql.parser;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlPrepareTable extends SqlDdl {

	private final SqlIdentifier name;

	private static final SqlOperator OPERATOR = new SqlSpecialOperator("PREPARE TABLE", SqlKind.OTHER_DDL);

	public SqlPrepareTable(SqlParserPos pos, SqlIdentifier name) {
		super(OPERATOR, pos);
		this.name = name;
	}

	public SqlIdentifier getName() {
		return name;
	}

	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableList.of(name);
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword("PREPARE");
		writer.keyword("TABLE");
		name.unparse(writer, leftPrec, rightPrec);
	}
}
