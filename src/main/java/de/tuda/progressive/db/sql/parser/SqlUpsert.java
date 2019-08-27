package de.tuda.progressive.db.sql.parser;

import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlUpsert extends SqlInsert {

	private final SqlNodeList indexColumns;

	private final SqlUpdate update;

	public SqlUpsert(
			SqlParserPos pos,
			SqlNode targetTable,
			SqlNodeList columnList,
			SqlNode[] values,
			SqlNodeList indexColumns,
			SqlUpdate update
	) {
		super(
				pos,
				SqlNodeList.EMPTY,
				targetTable,
				SqlUtils.getValues(values),
				columnList
		);

		this.indexColumns = indexColumns;
		this.update = update;
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		super.unparse(writer, leftPrec, rightPrec);

		final int opLeft = getOperator().getLeftPrec();
		final int opRight = getOperator().getRightPrec();

		writer.newlineAndIndent();
		writer.print("ON CONFLICT");
		writer.setNeedWhitespace(true);
		indexColumns.unparse(writer, opLeft, opRight);

		writer.newlineAndIndent();
		writer.print("DO");
		writer.setNeedWhitespace(true);

		update.unparse(writer, leftPrec, rightPrec);
	}
}
