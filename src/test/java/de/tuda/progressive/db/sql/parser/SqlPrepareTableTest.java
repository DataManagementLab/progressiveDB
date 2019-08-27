package de.tuda.progressive.db.sql.parser;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SqlPrepareTableTest {

	private SqlParser.ConfigBuilder configBuilder;

	@BeforeEach
	void init() {
		configBuilder = SqlParser.configBuilder();
		configBuilder.setParserFactory(SqlParserImpl.FACTORY);
	}

	@Test
	void valid() throws Exception {
		final String table = "test";
		SqlParser parser = SqlParser.create(String.format("prepare table %s", table), configBuilder.build());
		SqlNode node = parser.parseStmt();

		assertEquals(SqlPrepareTable.class, node.getClass());
		SqlPrepareTable prepareTable = (SqlPrepareTable) node;
		assertEquals(table, prepareTable.getName().getSimple().toLowerCase());
	}

	@Test
	void tableMissing() {
		final String table = "test";
		SqlParser parser = SqlParser.create(String.format("prepare %s", table), configBuilder.build());
		Assertions.assertThrows(SqlParseException.class, parser::parseStmt);
	}

	@Test
	void idMissing() {
		SqlParser parser = SqlParser.create("prepare table", configBuilder.build());
		Assertions.assertThrows(SqlParseException.class, parser::parseStmt);
	}

	@Test
	void notEnd() {
		SqlParser parser = SqlParser.create("prepare table foo bar", configBuilder.build());
		Assertions.assertThrows(SqlParseException.class, parser::parseStmt);
	}
}
