package de.tuda.progressive.db;

import de.tuda.progressive.db.sql.parser.SqlUpsert;
import de.tuda.progressive.db.util.SqlUtils;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlDdlNodes;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Utils {

	public static SqlCreateTable createTable(String table, int count, int keys) {
		return SqlDdlNodes.createTable(
				SqlParserPos.ZERO,
				false,
				false,
				new SqlIdentifier(table, SqlParserPos.ZERO),
				getColumns(table, count, keys),
				null
		);
	}

	private static SqlNodeList getColumns(String table, int count, int keys) {
		List<SqlNode> columns = IntStream.range(0, count)
				.mapToObj(i -> {
					SqlTypeName type;
					int precision;
					if (i < count - keys) {
						type = SqlTypeName.INTEGER;
						precision = 8;
					} else {
						type = SqlTypeName.VARCHAR;
						precision = 100;
					}

					return SqlUtils.createColumn(String.valueOf((char) ('a' + i)), type, precision, 0);
				})
				.collect(Collectors.toList());

		if (keys > 0) {
			columns.add(SqlDdlNodes.primary(
					SqlParserPos.ZERO,
					new SqlIdentifier("pk_" + table, SqlParserPos.ZERO),
					getIdentifiers(count - keys, keys)
			));
		}

		return new SqlNodeList(
				columns,
				SqlParserPos.ZERO
		);
	}

	private static SqlNodeList getIdentifiers(int count) {
		return getIdentifiers(0, count);
	}

	private static SqlNodeList getIdentifiers(int start, int count) {
		return new SqlNodeList(
				IntStream.range(start, start + count)
						.mapToObj(c -> new SqlIdentifier(String.valueOf((char) ('a' + c)), SqlParserPos.ZERO))
						.collect(Collectors.toList()),
				SqlParserPos.ZERO
		);
	}

	private static SqlNode[] getDynamicParams(int count) {
		return IntStream.range(0, count)
				.mapToObj(i -> new SqlDynamicParam((char) ('a' + i), SqlParserPos.ZERO))
				.collect(Collectors.toList())
				.toArray(new SqlNode[count]);
	}

	private static SqlNodeList getUpdates(int count, int keys) {
		return new SqlNodeList(
				IntStream.range(0, count)
						.mapToObj(i -> {
							if (i < count - keys) {
								return new SqlBasicCall(
										SqlStdOperatorTable.PLUS,
										new SqlNode[]{
												new SqlIdentifier(String.valueOf((char) ('a' + i)), SqlParserPos.ZERO),
												new SqlDynamicParam(count + i, SqlParserPos.ZERO)
										},
										SqlParserPos.ZERO
								);
							} else {
								return new SqlDynamicParam(count + i, SqlParserPos.ZERO);
							}
						})
						.collect(Collectors.toList()),
				SqlParserPos.ZERO
		);
	}

	public static SqlUpsert createUpsert(String table, int values, int keys) {
		return new SqlUpsert(
				SqlParserPos.ZERO,
				new SqlIdentifier(table, SqlParserPos.ZERO),
				getIdentifiers(values),
				getDynamicParams(values),
				getIdentifiers(values - keys, keys),
				new SqlUpdate(
						SqlParserPos.ZERO,
						new SqlIdentifier(Collections.emptyList(), SqlParserPos.ZERO),
						getIdentifiers(values),
						getUpdates(values, keys),
						null,
						null,
						null
				)
		);
	}
}
