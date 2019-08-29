package de.tuda.progressive.db.sql.parser;

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;

public class SqlCreateProgressiveView extends SqlCreate {

	private static final SqlOperator OPERATOR = new SqlSpecialOperator("CREATE PROGRESSIVE VIEW", SqlKind.OTHER_DDL);

	private final SqlIdentifier name;
	private final SqlNodeList columnList;
	private final SqlNode query;

	public SqlCreateProgressiveView(SqlParserPos pos, boolean replace, SqlIdentifier name, SqlNodeList columnList, SqlNode query) {
		super(OPERATOR, pos, replace, false);

		this.name = Objects.requireNonNull(name);
		this.columnList = columnList; // may be null
		this.query = Objects.requireNonNull(query);
	}

	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.of(name, columnList, query);
	}

	public SqlIdentifier getName() {
		return name;
	}

	public SqlNodeList getColumnList() {
		return columnList;
	}

	public SqlNode getQuery() {
		return query;
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		if (getReplace()) {
			writer.keyword("CREATE OR REPLACE");
		} else {
			writer.keyword("CREATE");
		}
		writer.keyword("PROGRESSIVE VIEW");
		name.unparse(writer, leftPrec, rightPrec);
		if (columnList != null) {
			SqlWriter.Frame frame = writer.startList("(", ")");
			for (SqlNode c : columnList) {
				writer.sep(",");
				c.unparse(writer, 0, 0);
			}
			writer.endList(frame);
		}
		writer.keyword("AS");
		writer.newlineAndIndent();
		query.unparse(writer, 0, 0);
	}
}
