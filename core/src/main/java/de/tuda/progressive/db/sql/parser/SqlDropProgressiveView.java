package de.tuda.progressive.db.sql.parser;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlDropProgressiveView extends SqlDrop {

	private static final SqlOperator OPERATOR = new SqlSpecialOperator("DROP PROGRESSIVE VIEW", SqlKind.OTHER_DDL);

	protected final SqlIdentifier name;

	SqlDropProgressiveView(SqlParserPos pos, boolean ifExists, SqlIdentifier name) {
		super(OPERATOR, pos, ifExists);

		this.name = name;
	}

	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableList.of(name);
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword(getOperator().getName());
		if (ifExists) {
			writer.keyword("IF EXISTS");
		}
		name.unparse(writer, leftPrec, rightPrec);
	}

	public SqlIdentifier getName() {
		return name;
	}

	public boolean isIfExists() {
		return ifExists;
	}
}
